# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


import asyncio
import contextlib
import re
import signal
import unicodedata
from datetime import datetime, timezone
import logging
from typing import Optional
import discord
from discord import ChannelType, MessageType
from discord.errors import Forbidden, HTTPException
import os
from discord.ext import commands
from common.config import Config, CURRENT_VERSION
from common.db import DBManager
from client.sitemap import SitemapService
from client.message_utils import MessageUtils
from common.websockets import WebsocketManager, AdminBus
from common.common_helpers import resolve_mapping_settings
from client.scraper import MemberScraper
from client.helpers import ClientUiController
from client.export_runners import (
    BackfillEngine,
    ExportMessagesRunner,
    DmHistoryExporter,
)


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
for lib in ("discord.state", "discord.client"):
    logging.getLogger(lib).setLevel(logging.ERROR)

logger = logging.getLogger("client")
logger.setLevel(LEVEL)


class ClientListener:
    def __init__(self):
        self.config = Config(logger=logger)
        self.db = DBManager(self.config.DB_PATH)
        self._mapped_original_ids: set[int] = set(self.db.get_all_original_guild_ids())
        self.blocked_keywords_map = self.db.get_blocked_keywords_by_origin()
        self._rebuild_blocklist(self.blocked_keywords_map)
        self.start_time = datetime.now(timezone.utc)
        self.bot = commands.Bot(command_prefix="!", self_bot=True)
        self.msg = MessageUtils(self.bot)
        self._sync_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._m_user = re.compile(r"<@!?(\d+)>")
        self.scraper = getattr(self, "scraper", None)
        self._scrape_lock = getattr(self, "_scrape_lock", asyncio.Lock())
        self._last_cancel_at: float | None = None
        self._cancelling: bool = False
        self._scrape_task = None
        self._scrape_gid = None
        self.do_precount = True
        self._dm_export_lock = asyncio.Lock()
        self._dm_export_task: asyncio.Task | None = None
        self._dm_export_running: bool = False
        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)
        self.bot.event(self.on_message_edit)
        self.bot.event(self.on_raw_message_edit)
        self.bot.event(self.on_message_delete)
        self.bot.event(self.on_raw_message_delete)
        self.bot.event(self.on_guild_channel_create)
        self.bot.event(self.on_guild_channel_delete)
        self.bot.event(self.on_guild_channel_update)
        self.bot.event(self.on_thread_delete)
        self.bot.event(self.on_thread_update)
        self.bot.event(self.on_member_join)
        self.bot.event(self.on_guild_role_create)
        self.bot.event(self.on_guild_role_delete)
        self.bot.event(self.on_guild_role_update)
        self.bot.event(self.on_guild_join)
        self.bot.event(self.on_guild_remove)
        self.bot.event(self.on_guild_update)
        self.bot.event(self.on_guild_emojis_update)
        self.bot.event(self.on_guild_stickers_update)
        self.bus = AdminBus(
            role="client", logger=logger, admin_ws_url=self.config.ADMIN_WS_URL
        )
        self.ws = WebsocketManager(
            send_url=self.config.SERVER_WS_URL,
            listen_host=self.config.CLIENT_WS_HOST,
            listen_port=self.config.CLIENT_WS_PORT,
            logger=logger,
        )
        self.sitemap = SitemapService(
            bot=self.bot, config=self.config, db=self.db, ws=self.ws, logger=logger
        )
        self.ui_controller = ClientUiController(
            bus=self.bus,
            admin_base_url=self.config.ADMIN_WS_URL,
            bot=self.bot,
            listener=self,
            logger=logging.getLogger("client.ui"),
        )
        self.runner = ExportMessagesRunner(
            bot=self.bot, ws=self.ws, msg_serializer=self.msg.serialize, logger=logger
        )
        self.backfill = BackfillEngine(self, logger=logger)
        self._bf_max = int(os.getenv("BACKFILL_MAX_CONCURRENT", "2"))
        self._bf_queue: asyncio.Queue[tuple[int, dict]] = asyncio.Queue(
            maxsize=int(os.getenv("BACKFILL_QUEUE_MAX", "500"))
        )
        self._bf_active: set[int] = set()
        self._bf_queued: set[int] = set()
        self._bf_worker_task: asyncio.Task | None = None
        self._bf_waiters: dict[int, asyncio.Event] = {}
        self._bf_pull_gate = asyncio.Semaphore(1)

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.bot.close())
            )

    def _settings_for_origin(self, guild_id: int | None) -> dict:
        if not guild_id:
            return self.config.default_mapping_settings()
        return resolve_mapping_settings(
            self.db, self.config, original_guild_id=int(guild_id)
        )

    def _is_mapped_origin(self, guild_id: int | None) -> bool:
        try:
            return bool(guild_id and int(guild_id) in self._mapped_original_ids)
        except Exception:
            return False

    def _reload_mapped_ids(self) -> None:
        try:
            self._mapped_original_ids = set(self.db.get_all_original_guild_ids())
            logger.debug(
                "[🔁] Reloaded mapped origins: %s", sorted(self._mapped_original_ids)
            )
        except Exception:
            logger.exception("Failed reloading mapped origins")

    async def _on_ws(self, msg: dict) -> dict | None:
        """
        Handles WebSocket (WS) messages received by the client.
        """
        typ = msg.get("type")
        data = msg.get("data", {})

        if typ == "mappings_reload":
            self._reload_mapped_ids()
            asyncio.create_task(self.sitemap.build_and_send_all())
            return {"ok": True, "mapped": list(self._mapped_original_ids)}

        elif typ == "settings_update":
            kw_map = data.get("blocked_keywords_map") or {}
            self._rebuild_blocklist(kw_map)

            total_words = sum(len(v) for v in self.blocked_keywords_map.values())
            logger.info(
                "[⚙️] Updated block list: %s guild scopes / %s total keywords",
                len(self.blocked_keywords_map),
                total_words,
            )
            return

        elif typ == "ping":
            now = datetime.now(timezone.utc)
            now_ts = now.timestamp()
            ws_latency = getattr(self.bot, "latency", None) or 0.0

            server_ts = data.get("timestamp")
            round_trip = (now_ts - server_ts) if server_ts else None

            return {
                "data": {
                    "client_timestamp": now_ts,
                    "discord_ws_latency_s": ws_latency,
                    "round_trip_seconds": round_trip,
                    "client_start_time": self.start_time.isoformat(),
                },
            }
        elif typ == "filters_reload":
            self.config._load_filters_from_db()
            logger.info("[⚙️] Filters reloaded from DB")
            try:
                self.sitemap.reload_filters_and_resend()
            except AttributeError:
                asyncio.create_task(self.sitemap.build_and_send_all())
            return {"ok": True}

        elif typ == "clone_messages":
            chan_id = int(data.get("channel_id"))
            rng = (data.get("range") or {}) if isinstance(data, dict) else {}
            mode = (data.get("mode") or rng.get("mode") or "").lower()

            after_iso = (
                data.get("after_iso")
                or data.get("since")
                or (rng.get("value") if mode == "since" else None)
            )
            before_iso = (
                data.get("before_iso")
                or (rng.get("before") if mode in ("between", "range") else None)
                or data.get("until")
            )
            _n = data.get("last_n") or (
                rng.get("value") if mode in ("last", "last_n") else None
            )
            try:
                last_n = int(_n) if _n is not None else None
            except Exception:
                last_n = None

            resume = bool(data.get("resume"))
            after_id = data.get("after_id")

            params = {
                "after_iso": after_iso,
                "before_iso": before_iso,
                "last_n": last_n,
                "resume": resume,
                "after_id": after_id,
                "mapping_id": data.get("mapping_id"),
                "cloned_guild_id": data.get("cloned_guild_id"),
                "original_guild_id": data.get("original_guild_id"),
            }
            return await self._enqueue_backfill(chan_id, params)

        elif typ == "backfill_done":
            data = msg.get("data") or {}
            cid_raw = data.get("channel_id")
            try:
                cid = int(cid_raw)
            except (TypeError, ValueError):
                return
            ev = self._bf_waiters.get(cid)
            if ev:
                ev.set()
            self._bf_active.discard(cid)

            return

        elif typ == "backfills_queue_query":

            try:

                pending = list(getattr(self._bf_queue, "_queue", []))
            except Exception:
                pending = []

            pending_items = []
            for idx, tup in enumerate(pending, start=1):
                try:
                    cid = int(tup[0])
                except Exception:
                    continue
                pending_items.append(
                    {
                        "channel_id": str(cid),
                        "position": idx,
                        "state": "queued",
                    }
                )

            active_items = []
            for cid in list(self._bf_active):
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                active_items.append(
                    {
                        "channel_id": str(cid_int),
                        "position": 0,
                        "state": "active",
                    }
                )

            return {
                "type": "backfills_queue",
                "data": {"items": active_items + pending_items},
                "ok": True,
            }

        elif typ == "sitemap_request":
            target_gid = None
            try:
                target_gid = int((data or {}).get("guild_id"))
            except Exception:
                target_gid = None

            self.schedule_sync(guild_id=target_gid)
            logger.info("[🌐] Received sitemap request for %s", target_gid or "ALL")
            return {"ok": True}

        elif typ == "scrape_members":
            data = data or {}

            inc_username = bool(data.get("include_username", False))
            inc_avatar_url = bool(data.get("include_avatar_url", False))
            inc_bio = bool(data.get("include_bio", False))
            inc_roles = bool(data.get("include_roles", False))
            gid = str(data.get("guild_id") or "")
            self._scrape_gid = gid

            def clamp(v, lo, hi):
                return max(lo, min(hi, v))

            try:
                ns = int(data.get("num_sessions", 2))
            except Exception:
                ns = 2
            ns = clamp(ns, 1, 5)

            mpps = data.get("max_parallel_per_session")
            if mpps is None:
                mpps = clamp(8 // ns, 1, 5)
            else:
                try:
                    mpps = clamp(int(mpps), 1, 5)
                except Exception:
                    mpps = 1

            def _err_msg(e: BaseException) -> str:
                msg = str(e).strip()
                return msg or type(e).__name__

            try:
                if self.scraper is None:
                    self.scraper = MemberScraper(self.bot, self.config, logger=logger)

                async with self._scrape_lock:
                    if self._scrape_task and not self._scrape_task.done():

                        return {"ok": False, "error": "scrape-already-running"}

                    try:
                        await self.bus.publish(
                            kind="client",
                            payload={
                                "type": "scrape_started",
                                "data": {
                                    "guild_id": gid,
                                    "options": {
                                        "include_username": inc_username,
                                        "include_avatar_url": inc_avatar_url,
                                        "include_bio": inc_bio,
                                        "num_sessions": ns,
                                        "max_parallel_per_session": mpps,
                                        "include_roles": inc_roles,
                                    },
                                },
                            },
                        )
                    except Exception:
                        pass

                try:
                    target_gid = int(gid) if gid else None
                except Exception:
                    target_gid = None

                self._scrape_task = asyncio.create_task(
                    self.scraper.scrape(
                        guild_id=target_gid,
                        include_username=inc_username,
                        include_avatar_url=inc_avatar_url,
                        include_bio=inc_bio,
                        include_roles=inc_roles,
                        num_sessions=ns,
                        max_parallel_per_session=mpps,
                    ),
                    name="scrape",
                )

                async def _finish_scrape():
                    try:
                        result = await self._scrape_task
                        count = len((result or {}).get("members", []))
                        logger.debug("[scrape] TASK_DONE count=%d", count)

                        import os, json, datetime as _dt

                        scrapes_dir = "/data/scrapes"
                        os.makedirs(scrapes_dir, exist_ok=True)

                        rgid = str((result or {}).get("guild_id") or gid or "unknown")
                        gname = (result or {}).get("guild_name", "guild")
                        slug = "".join(
                            ch if ch.isalnum() else "_" for ch in gname
                        ).strip("_")
                        while "__" in slug:
                            slug = slug.replace("__", "_")

                        ts = _dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                        outfile = os.path.join(scrapes_dir, f"{slug}_{rgid}_{ts}.json")

                        with open(outfile, "w", encoding="utf-8") as f:
                            json.dump(result or {}, f, ensure_ascii=False, indent=2)

                        try:
                            await self.bus.publish(
                                kind="client",
                                payload={
                                    "type": "scrape_done",
                                    "data": {
                                        "guild_id": rgid,
                                        "count": count,
                                        "path": outfile,
                                        "filename": os.path.basename(outfile),
                                    },
                                },
                            )
                        except Exception:
                            pass

                    except asyncio.CancelledError:

                        import os, json, datetime as _dt

                        snap = await self.scraper.snapshot_members()
                        try:
                            rgid = str(self._scrape_gid or gid or "unknown")
                            try:
                                g = self.bot.get_guild(int(rgid))
                            except Exception:
                                g = None
                            gname = g.name if g else "guild"

                            scrapes_dir = "/data/scrapes"
                            os.makedirs(scrapes_dir, exist_ok=True)

                            slug = "".join(
                                ch if ch.isalnum() else "_" for ch in gname
                            ).strip("_")
                            while "__" in slug:
                                slug = slug.replace("__", "_")

                            ts = _dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                            outfile = os.path.join(
                                scrapes_dir, f"{slug}_{rgid}_{ts}.json"
                            )

                            with open(outfile, "w", encoding="utf-8") as f:
                                json.dump(
                                    {
                                        "members": snap,
                                        "count": len(snap),
                                        "guild_id": rgid,
                                        "guild_name": gname,
                                    },
                                    f,
                                    ensure_ascii=False,
                                    indent=2,
                                )

                            await self.bus.publish(
                                kind="client",
                                payload={
                                    "type": "scrape_done",
                                    "data": {
                                        "guild_id": rgid,
                                        "count": len(snap),
                                        "path": outfile,
                                        "filename": os.path.basename(outfile),
                                        "partial": True,
                                    },
                                },
                            )
                        except Exception:

                            try:
                                await self.bus.publish(
                                    kind="client",
                                    payload={
                                        "type": "scrape_cancelled",
                                        "data": {"guild_id": self._scrape_gid or gid},
                                    },
                                )
                            except Exception:
                                pass

                    except BaseException as e:
                        logger.exception("[❌] OP8 scrape failed: %r", e)
                        try:
                            await self.bus.publish(
                                kind="client",
                                payload={
                                    "type": "scrape_failed",
                                    "data": {"guild_id": gid, "error": _err_msg(e)},
                                },
                            )
                        except Exception:
                            pass
                    finally:
                        self._scrape_task = None
                        self._scrape_gid = None

                asyncio.create_task(_finish_scrape())

                return {"ok": True, "accepted": True, "guild_id": gid}

            except BaseException as e:
                logger.exception("[❌] OP8 scrape failed (outer): %r", e)
                try:
                    await self.bus.publish(
                        kind="client",
                        payload={
                            "type": "scrape_failed",
                            "data": {"guild_id": gid, "error": _err_msg(e)},
                        },
                    )
                except Exception:
                    pass
                return {"ok": False, "error": _err_msg(e)}

        elif typ == "scrape_status":

            running = bool(self._scrape_task and not self._scrape_task.done())
            return {"ok": True, "running": running, "guild_id": self._scrape_gid}

        elif typ == "scrape_cancel":
            req_gid = str((data or {}).get("guild_id") or "")
            is_running = bool(self._scrape_task and not self._scrape_task.done())
            if not is_running:
                return {"ok": False, "error": "no-scrape-running"}

            try:
                if self.scraper:
                    self.scraper.request_cancel()

                try:
                    self._scrape_task.cancel()
                except Exception:
                    pass

                try:
                    await self.bus.publish(
                        kind="client",
                        payload={
                            "type": "scrape_cancelled",
                            "data": {"guild_id": self._scrape_gid or req_gid},
                        },
                    )
                except Exception:
                    pass
                return {"ok": True, "cancelling": True}
            except Exception as e:
                logger.exception("[scrape_cancel] failed: %r", e)
                return {"ok": False, "error": str(e)}

        elif typ == "export_dm_history":
            uid = int(data["user_id"])
            webhook_url = (data.get("webhook_url") or "").strip() or None

            def _as_bool(v, default=True):
                if v is None:
                    return default
                if isinstance(v, bool):
                    return v
                return str(v).strip().lower() in ("1", "true", "yes", "on")

            save_json = _as_bool(data.get("json_file"), default=True)

            acquired = await DmHistoryExporter.try_begin(uid)
            if not acquired:
                return {"ok": False, "error": "dm-export-in-progress", "user_id": uid}

            async def _export():
                try:
                    exporter = DmHistoryExporter(
                        bot=self.bot,
                        ws=self.ws,
                        msg_serializer=self.msg.serialize,
                        logger=logger.getChild("dm_export"),
                        send_sleep=2.0,
                        do_precache_count=True,
                        out_root="/data/exports",
                        save_json=save_json,
                    )
                    await exporter.run(user_id=uid, webhook_url=webhook_url)
                finally:
                    await DmHistoryExporter.end(uid)

            task = asyncio.create_task(_export())
            await DmHistoryExporter.register_task(uid, task)

            return {"ok": True, "user_id": uid}

        elif typ == "export_messages":
            d = data or {}
            gid = str(d.get("guild_id") or "").strip() or None

            try:
                target_gid = int(gid) if gid else None
            except Exception:
                target_gid = None

            guild = self.bot.get_guild(target_gid) if target_gid else None
            if guild is None and self.bot.guilds:
                guild = self.bot.guilds[0]
            if guild is None:
                return {"ok": False, "error": "Guild not found"}

            g_id = getattr(guild, "id", None)
            if g_id is None:
                return {"ok": False, "error": "Guild ID not found"}

            acquired = await self.runner.try_begin(g_id)
            if not acquired:
                return {"ok": False, "error": "Export already running for this guild"}

            asyncio.create_task(self.runner.run(d, guild, acquired=True))
            return {"ok": True, "accepted": True}

        elif typ == "pull_assets":
            import logging

            try:
                from export_runners import AssetExportRunner
            except Exception:
                from .export_runners import AssetExportRunner

            try:
                req_gid_val = (data or {}).get("guild_id")
                req_gid = int(req_gid_val) if req_gid_val is not None else 0
            except Exception:
                req_gid = 0

            req_gid_val = (data or {}).get("guild_id")
            try:
                gid = int(req_gid_val) if req_gid_val is not None else 0
            except Exception:
                gid = 0

            if not gid:
                gid = (
                    next(iter(self._mapped_original_ids))
                    if self._mapped_original_ids
                    else 0
                )
            if not gid:
                return {"ok": False, "reason": "no-mapped-origin"}

            guild = self.bot.get_guild(int(gid))
            if guild is None:
                return {"ok": False, "reason": f"not-in-guild:{gid}"}

            sel = str(((data or {}).get("asset") or "both")).lower()
            include_emojis = sel in ("emojis", "both")
            include_stickers = sel in ("stickers", "both")

            runner = AssetExportRunner(
                self.bot,
                self.ws,
                logger=(
                    getattr(self, "logger", None) or logging.getLogger("asset_export")
                ),
            )
            res = await runner.run(
                guild, include_emojis=include_emojis, include_stickers=include_stickers
            )

            return {"ok": True, **res}

        return None

    async def _resolve_accessible_host_channel(self, orig_channel_id: int):
        """
        Maps a cloned channel id to its host channel id (if applicable), and returns a
        channel object you can actually access along with the resolved channel_id and guild.

        Returns: (channel: discord.TextChannel, channel_id: int, guild: discord.Guild)
        Raises: discord.Forbidden if no accessible channel can be found.
        """
        logger = logging.getLogger("client")

        channel_id = int(orig_channel_id)
        if hasattr(self, "chan_map"):

            def _row_get(row, key, default=None):
                try:
                    if isinstance(row, dict):
                        return row.get(key, default)
                    return row[key] if key in row.keys() else default
                except Exception:
                    return default

            for src_id, row in getattr(self, "chan_map", {}).items():
                if int(_row_get(row, "cloned_channel_id", 0) or 0) == channel_id:
                    logger.debug(
                        f"[map] Mapped cloned channel {channel_id} -> host channel {src_id}"
                    )
                    channel_id = int(src_id)
                    break

        channel = self.bot.get_channel(channel_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except discord.Forbidden:
                channel = None

        guild = getattr(channel, "guild", None)
        if guild is None:
            host_guild = next(
                (
                    self.bot.get_guild(gid)
                    for gid in sorted(self._mapped_original_ids)
                    if self.bot.get_guild(gid)
                ),
                None,
            )
            if host_guild is None and self.bot.guilds:
                host_guild = self.bot.guilds[0]
            guild = host_guild

        if channel is None:
            me = guild.me or guild.get_member(
                getattr(getattr(self.bot, "user", None), "id", 0)
            )

            def can_read(ch) -> bool:
                try:
                    if me is None:
                        return False
                    perms = ch.permissions_for(me)
                    return bool(perms.view_channel and perms.read_message_history)
                except Exception:
                    return False

            readable = next((ch for ch in guild.text_channels if can_read(ch)), None)
            if not readable:
                raise discord.Forbidden(
                    None,
                    {"message": "No accessible text channel found in the host guild."},
                )

            channel = readable
            channel_id = readable.id

        return channel, channel_id, guild

    async def periodic_sync_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        while True:
            try:
                await self.sitemap.build_and_send_all()
            except Exception:
                logger.exception("Error in periodic sync loop")
            await asyncio.sleep(self.config.SYNC_INTERVAL_SECONDS)

    def schedule_sync(self, guild_id: int | None = None, delay: float = 1.0):
        """
        Ask SitemapService to (debounced) resend sitemap(s).

        guild_id:
        - int -> only that origin guild's sitemap will be rebuilt/sent
        - None -> fallback: mark all mapped origins dirty (legacy "send everything")
        """
        try:
            self.sitemap.schedule_sync(guild_id=guild_id, delay=delay)
        except TypeError:
            self.sitemap.schedule_sync(None, delay=delay)
        except Exception:
            logger.exception("[sitemap] failed to schedule sync for %s", guild_id)

    async def _disable_cloning(self, reason: str = ""):
        logger.info("[🔕] Disabling server cloning: %s", reason or "(no reason)")
        self.config.ENABLE_CLONING = False
        if self._sync_task:
            try:
                self._sync_task.cancel()
            except Exception:
                pass
            self._sync_task = None

    async def on_ready(self):

        self._reload_mapped_ids()

        mapped_origins = set(self._mapped_original_ids)

        active_enabled_origin_ids = []
        for g in self.bot.guilds:
            if g.id in mapped_origins:
                s = self._settings_for_origin(g.id)
                if s.get("ENABLE_CLONING", True):
                    active_enabled_origin_ids.append(g.id)

        has_active_enabled_origins = bool(active_enabled_origin_ids)

        asyncio.create_task(self.config.setup_release_watcher(self, should_dm=False))
        self.ui_controller.start()

        who = getattr(getattr(self.bot, "user", None), "display_name", "(unknown)")
        msg = f"Logged in as {who}"
        await self.bus.status(
            running=True,
            status=msg,
            discord={"ready": True},
        )
        logger.info("[🤖] %s", msg)

        if has_active_enabled_origins:
            if self._sync_task is None:
                self._sync_task = asyncio.create_task(self.periodic_sync_loop())
        else:
            logger.info(
                "[🔕] No eligible mapped origin guilds with cloning enabled; "
                "skipping sync."
            )

        if self._ws_task is None:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))

        asyncio.create_task(self._snapshot_all_guilds_once())

    def _rebuild_blocklist(self, kw_map: dict | None = None) -> None:
        """
        kw_map: { origin_guild_id (int/str or 0): ["badword", ...], ... }

        After this runs:
        self.blocked_keywords_map[guild_id] = ["badword", "otherword", ...]
        self._blocked_patterns_map[guild_id] = [(compiled_regex, "badword"), ...]
        """
        if kw_map is None:
            kw_map = self.db.get_blocked_keywords_by_origin()

        normalized_map: dict[int, list[str]] = {}
        patterns_map: dict[int, list[tuple[re.Pattern, str]]] = {}

        for gid_key, words in (kw_map or {}).items():
            try:
                gid_int = int(gid_key)
            except (TypeError, ValueError):
                continue

            cleaned_words = [
                (w or "").strip().lower() for w in (words or []) if w and str(w).strip()
            ]

            normalized_map[gid_int] = cleaned_words

            pat_list: list[tuple[re.Pattern, str]] = []
            for w in cleaned_words:
                regex = re.compile(
                    rf"(?<!\w){re.escape(w)}(?!\w)",
                    re.IGNORECASE,
                )
                pat_list.append((regex, w))

            patterns_map[gid_int] = pat_list

        self.blocked_keywords_map = normalized_map
        self._blocked_patterns_map = patterns_map

        logger.debug("[⚙️] Block list now: %s", self.blocked_keywords_map)

    def should_ignore(self, message: discord.Message) -> bool:
        """
        Determines whether a given Discord message should be ignored based on various conditions.
        """
        ch = message.channel
        try:
            if isinstance(ch, discord.Thread):
                if not self.sitemap.in_scope_thread(ch):
                    return True
            else:
                if not self.sitemap.in_scope_channel(ch):
                    return True
        except Exception:
            pass

        if message.type == MessageType.thread_created:
            return True

        if message.type == MessageType.channel_name_change:
            return True

        g = getattr(message, "guild", None)
        if not g or not self._is_mapped_origin(g.id):
            return True
        if not self._settings_for_origin(g.id).get("ENABLE_CLONING", True):
            return True

        if message.channel.type in (ChannelType.voice, ChannelType.stage_voice):

            return True

        content = unicodedata.normalize("NFKC", message.content or "")

        g = getattr(message, "guild", None)
        guild_id = getattr(g, "id", None)
        guild_name = getattr(g, "name", "unknown")

        patterns_to_check: list[tuple[re.Pattern, str]] = []

        patterns_to_check.extend(self._blocked_patterns_map.get(0, []))

        if guild_id is not None:
            try:
                gid_int = int(guild_id)
                patterns_to_check.extend(self._blocked_patterns_map.get(gid_int, []))
            except (TypeError, ValueError):
                pass

        for pat, kw in patterns_to_check:
            if pat.search(content):
                logger.info(
                    "[❌] Dropping message %s in %s: blocked keyword (%s)",
                    message.id,
                    guild_name,
                    kw,
                )
                return True

        return False

    async def maybe_send_announcement(self, message: discord.Message) -> bool:
        content = message.content
        lower = content.lower()
        author = message.author
        chan_id = message.channel.id
        guild_id = message.guild.id if message.guild else 0

        triggers = self.db.get_effective_announcement_triggers(guild_id)
        if not triggers:
            return False

        for kw, entries in triggers.items():
            key = kw.lower()
            matched = False

            if re.match(r"^\w+$", key) and re.search(rf"\b{re.escape(key)}\b", lower):
                matched = True

            if (
                not matched
                and re.match(r"^[A-Za-z0-9_]+$", key)
                and re.search(rf"<a?:{re.escape(key)}:\d+>", content)
            ):
                matched = True

            if not matched and key in lower:
                matched = True

            if not matched:
                continue

            for filter_id, allowed_chan in entries:
                if (filter_id == 0 or author.id == filter_id) and (
                    allowed_chan == 0 or chan_id == allowed_chan
                ):
                    payload = {
                        "type": "announce",
                        "data": {
                            "guild_id": guild_id,
                            "keyword": kw,
                            "content": content,
                            "author": author.name,
                            "channel_id": chan_id,
                            "channel_name": getattr(
                                message.channel, "name", str(chan_id)
                            ),
                            "timestamp": str(message.created_at),
                        },
                    }
                    await self.ws.send(payload)
                    logger.info(
                        f"[📢] Announcement `{kw}` by {author} in g={guild_id}."
                    )
                    return True

        return False

    async def _resolve_forward_chain(
        self, wrapper_msg: discord.Message, max_depth: int = 4
    ):
        """
        Try to unwrap a chain of forwarded/quoted messages until we find one
        that actually has usable content.
        """
        seen = 0
        current = wrapper_msg

        while seen < max_depth and current is not None:

            has_text = bool(
                (current.content or "").strip()
                or (getattr(current, "system_content", "") or "").strip()
            )
            has_atts = bool(getattr(current, "attachments", None))
            has_embs = bool(getattr(current, "embeds", None))
            has_stks = bool(getattr(current, "stickers", None))

            if has_text or has_atts or has_embs or has_stks:

                return current

            ref = getattr(current, "reference", None)
            if not ref:
                break

            next_msg = None

            ch = None
            try:
                ch = self.bot.get_channel(int(ref.channel_id))
            except Exception:
                ch = None

            if ch is None:
                try:
                    ch = await self.bot.fetch_channel(int(ref.channel_id))
                except Exception:
                    ch = None

            if ch is None:
                break

            try:
                next_msg = await ch.fetch_message(int(ref.message_id))
            except Exception:
                next_msg = None

            current = next_msg
            seen += 1

        return None

    async def on_message(self, message: discord.Message):
        """
        Handles incoming Discord messages and processes them for forwarding.
        This method is triggered whenever a message is sent in a channel the bot has access to.
        """
        g = getattr(message, "guild", None)
        if not g or not self._is_mapped_origin(g.id):
            return

        await self.maybe_send_announcement(message)

        settings = self._settings_for_origin(g.id)

        if not settings.get("ENABLE_CLONING", True):
            return

        if self.should_ignore(message):
            return

        raw = message.content or ""
        system = getattr(message, "system_content", "") or ""

        forwarded_flag_val = 0
        try:

            forwarded_flag_val = int(
                getattr(getattr(message, "flags", 0), "value", 0) or 0
            )
        except Exception:
            pass

        looks_like_forward = (not raw and not system) and (
            getattr(message, "reference", None) or (forwarded_flag_val & 16384)
        )

        # We'll call the thing we actually serialize "src_msg".
        # By default it's just the incoming message.
        src_msg = message

        if looks_like_forward:
            resolved = await self._resolve_forward_chain(message)
            if resolved is not None:
                src_msg = resolved
            else:

                # So this is basically a forward-of-a-forward from a guild we can't read.
                logger.info(
                    "[↩️] Dropping unresolvable forward wrapper in #%s (no usable content)",
                    getattr(message.channel, "name", "?"),
                )
                return

        src_raw = src_msg.content or ""
        src_sys = getattr(src_msg, "system_content", "") or ""

        if not src_raw and src_sys:
            content = src_sys
            author = "System"
        else:
            content = src_raw
            author = (
                src_msg.author.name if getattr(src_msg, "author", None) else "System"
            )

        no_visible_text = content.strip() == ""
        no_attachments = not getattr(src_msg, "attachments", None)
        no_embeds = not getattr(src_msg, "embeds", None)
        no_stickers = not getattr(src_msg, "stickers", None)

        if no_visible_text and no_attachments and no_embeds and no_stickers:
            logger.info(
                "[🚫] Not forwarding empty content in #%s (even after resolve)",
                getattr(message.channel, "name", "?"),
            )
            return

        attachments = [
            {
                "url": att.url,
                "filename": att.filename,
                "size": att.size,
            }
            for att in getattr(src_msg, "attachments", [])
        ]

        raw_embeds = [e.to_dict() for e in getattr(src_msg, "embeds", [])]
        mention_map = await self.msg.build_mention_map(src_msg, raw_embeds)
        embeds = [
            self.msg.sanitize_embed_dict(e, src_msg, mention_map) for e in raw_embeds
        ]
        safe_content = self.msg.sanitize_inline(content, src_msg, mention_map)

        components: list[dict] = []
        for comp in getattr(src_msg, "components", []):
            try:
                components.append(comp.to_dict())
            except NotImplementedError:
                row: dict = {"type": getattr(comp, "type", None), "components": []}
                for child in getattr(comp, "children", []):
                    child_data: dict = {}
                    for attr in ("custom_id", "label", "style", "url", "disabled"):
                        if hasattr(child, attr):
                            child_data[attr] = getattr(child, attr)
                    if hasattr(child, "emoji") and child.emoji:
                        emoji = child.emoji
                        emoji_data: dict = {}
                        if hasattr(emoji, "name"):
                            emoji_data["name"] = emoji.name
                        if getattr(emoji, "id", None):
                            emoji_data["id"] = emoji.id
                        child_data["emoji"] = emoji_data
                    row["components"].append(child_data)
                components.append(row)

        # We don't want src_msg.channel (that might be a different guild).

        target_chan = message.channel
        target_guild = message.guild

        is_thread = target_chan.type in (
            ChannelType.public_thread,
            ChannelType.private_thread,
        )

        stickers_payload = self.msg.stickers_payload(getattr(src_msg, "stickers", []))

        data_block = {
            "guild_id": getattr(target_guild, "id", None),
            "message_id": getattr(src_msg, "id", None),
            "channel_id": target_chan.id,
            "channel_name": getattr(target_chan, "name", str(target_chan.id)),
            "channel_type": target_chan.type.value,
            "author": author,
            "author_id": getattr(getattr(src_msg, "author", None), "id", None),
            "avatar_url": (
                str(src_msg.author.display_avatar.url)
                if getattr(getattr(src_msg, "author", None), "display_avatar", None)
                else None
            ),
            "content": safe_content,
            "timestamp": str(getattr(src_msg, "created_at", None)),
            "attachments": attachments,
            "components": components,
            "stickers": stickers_payload,
            "embeds": embeds,
        }

        if is_thread:
            parent = getattr(target_chan, "parent", None)
            if parent is not None:
                data_block.update(
                    {
                        "thread_parent_id": parent.id,
                        "thread_parent_name": getattr(parent, "name", str(parent.id)),
                        "thread_id": target_chan.id,
                        "thread_name": getattr(
                            target_chan, "name", str(target_chan.id)
                        ),
                    }
                )

        payload = {
            "type": "thread_message" if is_thread else "message",
            "data": data_block,
        }

        await self.ws.send(payload)

        logger.info(
            "[📩] New msg detected in #%s from %s; forwarding to server",
            message.channel.name,
            message.author.name,
        )

    def _is_meaningful_edit(
        self, before: discord.Message, after: discord.Message
    ) -> bool:
        if (before.content or "") != (after.content or ""):
            return True
        if [(a.url, a.size) for a in before.attachments] != [
            (a.url, a.size) for a in after.attachments
        ]:
            return True
        if len(before.components) != len(after.components):
            return True
        if len(getattr(before, "stickers", [])) != len(getattr(after, "stickers", [])):
            return True
        try:
            if getattr(before, "flags", None) != getattr(after, "flags", None):
                return False
        except Exception:
            pass
        return False

    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """
        When an upstream message is edited, forward the new content/embeds/components.
        """
        g = getattr(after, "guild", None)
        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(g.id)

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("EDIT_MESSAGES", True):
            return

        if self.should_ignore(after):
            return

        if getattr(self.config, "IGNORE_EMBED_ONLY_EDITS", True):
            if not self._is_meaningful_edit(before, after):
                logger.debug("[edit] Ignoring embed-only/unfurl edit for %s", after.id)
                return

        raw = after.content or ""
        system = getattr(after, "system_content", "") or ""
        if not raw and system:
            content = system
            author = "System"
        else:
            content = raw
            author = after.author.name

        attachments = [
            {"url": att.url, "filename": att.filename, "size": att.size}
            for att in after.attachments
        ]

        raw_embeds = [e.to_dict() for e in after.embeds]
        mention_map = await self.msg.build_mention_map(after, raw_embeds)
        embeds = [
            self.msg.sanitize_embed_dict(e, after, mention_map) for e in raw_embeds
        ]
        content = self.msg.sanitize_inline(content, after, mention_map)

        components: list[dict] = []
        for comp in after.components:
            try:
                components.append(comp.to_dict())
            except NotImplementedError:
                row: dict = {"type": getattr(comp, "type", None), "components": []}
                for child in getattr(comp, "children", []):
                    child_data: dict = {}
                    for attr in ("custom_id", "label", "style", "url", "disabled"):
                        if hasattr(child, attr):
                            child_data[attr] = getattr(child, attr)
                    if hasattr(child, "emoji") and child.emoji:
                        emoji = child.emoji
                        emoji_data: dict = {}
                        if hasattr(emoji, "name"):
                            emoji_data["name"] = emoji.name
                        if getattr(emoji, "id", None):
                            emoji_data["id"] = emoji.id
                        child_data["emoji"] = emoji_data
                    row["components"].append(child_data)
                components.append(row)

        is_thread = after.channel.type in (
            ChannelType.public_thread,
            ChannelType.private_thread,
        )
        stickers_payload = self.msg.stickers_payload(getattr(after, "stickers", []))

        payload = {
            "type": "thread_message_edit" if is_thread else "message_edit",
            "data": {
                "guild_id": getattr(after.guild, "id", None),
                "message_id": getattr(after, "id", None),
                "channel_id": after.channel.id,
                "channel_name": getattr(after.channel, "name", str(after.channel.id)),
                "channel_type": after.channel.type.value,
                "author": author,
                "author_id": after.author.id,
                "avatar_url": (
                    str(after.author.display_avatar.url)
                    if after.author.display_avatar
                    else None
                ),
                "content": content,
                "timestamp": str(after.edited_at or after.created_at),
                "attachments": attachments,
                "components": components,
                "stickers": stickers_payload,
                "embeds": embeds,
                **(
                    {
                        "thread_parent_id": after.channel.parent.id,
                        "thread_parent_name": after.channel.parent.name,
                        "thread_id": after.channel.id,
                        "thread_name": after.channel.name,
                    }
                    if is_thread
                    else {}
                ),
            },
        }
        await self.ws.send(payload)
        logger.info(
            "[✏️] Message edit detected in #%s by %s → sent to server",
            payload["data"]["channel_name"],
            author,
        )

    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        gid = getattr(payload, "guild_id", None)
        if not gid or not self._is_mapped_origin(gid):
            return

        if payload.cached_message is not None:
            return

        settings = self._settings_for_origin(gid)

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("EDIT_MESSAGES", True):
            return

        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(payload.channel_id)
            except Exception:
                return

        if isinstance(channel, discord.Thread):
            if not self.sitemap.in_scope_thread(channel):
                return
        else:
            if not self.sitemap.in_scope_channel(channel):
                return

        msg = payload.cached_message
        data = payload.data or {}

        if getattr(self.config, "IGNORE_EMBED_ONLY_EDITS", True):
            changed = set(data.keys()) - {
                "id",
                "type",
                "guild_id",
                "channel_id",
                "edited_timestamp",
                "timestamp",
            }
            if changed <= {"embeds"}:
                logger.debug(
                    "[edit] Ignoring raw embed-only edit for %s", payload.message_id
                )
                return

        content = None
        embeds = None
        author = None
        author_id = None
        avatar_url = None
        timestamp = None

        if msg is None:
            try:
                msg = await channel.fetch_message(payload.message_id)
            except Exception:
                msg = None

        if msg:
            raw_embeds = [e.to_dict() for e in msg.embeds]
            mention_map = await self.msg.build_mention_map(msg, raw_embeds)
            embeds = [
                self.msg.sanitize_embed_dict(e, msg, mention_map) for e in raw_embeds
            ]
            content = self.msg.sanitize_inline(msg.content or "", msg, mention_map)
            author = getattr(getattr(msg, "author", None), "name", None)
            author_id = getattr(getattr(msg, "author", None), "id", None)
            avatar_url = (
                str(msg.author.display_avatar.url)
                if getattr(msg.author, "display_avatar", None)
                else None
            )
            timestamp = str(msg.edited_at or msg.created_at)
        else:
            content = data.get("content")
            embeds = data.get("embeds")
            a = data.get("author") or {}
            author = a.get("global_name") or a.get("username") or a.get("name")
            author_id = a.get("id")
            if a.get("id") and a.get("avatar"):
                avatar_url = (
                    f"https://cdn.discordapp.com/avatars/{a['id']}/{a['avatar']}.png"
                )
            timestamp = data.get("edited_timestamp") or data.get("timestamp")

        is_thread = getattr(channel, "type", None) in (
            discord.ChannelType.public_thread,
            discord.ChannelType.private_thread,
        )

        out = {
            "type": "thread_message_edit" if is_thread else "message_edit",
            "data": {
                "guild_id": payload.guild_id,
                "message_id": payload.message_id,
                "channel_id": payload.channel_id,
                "channel_name": getattr(channel, "name", str(payload.channel_id)),
                "channel_type": (
                    getattr(channel, "type", None).value
                    if getattr(channel, "type", None)
                    else None
                ),
                "author": author,
                "author_id": author_id,
                "avatar_url": avatar_url,
                "content": content,
                "timestamp": timestamp,
                "embeds": embeds,
                **(
                    {
                        "thread_parent_id": channel.parent.id,
                        "thread_parent_name": channel.parent.name,
                        "thread_id": channel.id,
                        "thread_name": channel.name,
                    }
                    if is_thread
                    else {}
                ),
            },
        }

        await self.ws.send(out)
        logger.info(
            "[✏️] Message edit detected in #%s → sent to server",
            out["data"]["channel_name"],
        )

    async def on_message_delete(self, message: discord.Message):
        """
        When an upstream message is deleted, tell the server to delete the cloned webhook message.
        """
        g = getattr(message, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(g.id)

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("DELETE_MESSAGES", True):
            return

        if self.should_ignore(message):
            return

        is_thread = message.channel.type in (
            ChannelType.public_thread,
            ChannelType.private_thread,
        )
        payload = {
            "type": "thread_message_delete" if is_thread else "message_delete",
            "data": {
                "guild_id": getattr(message.guild, "id", None),
                "message_id": getattr(message, "id", None),
                "channel_id": message.channel.id,
                "channel_name": getattr(
                    message.channel, "name", str(message.channel.id)
                ),
                "channel_type": message.channel.type.value,
                **(
                    {
                        "thread_parent_id": message.channel.parent.id,
                        "thread_parent_name": message.channel.parent.name,
                        "thread_id": message.channel.id,
                        "thread_name": message.channel.name,
                    }
                    if is_thread
                    else {}
                ),
            },
        }
        await self.ws.send(payload)
        logger.info(
            "[🗑️] Message delete detected in #%s → sent to server",
            payload["data"]["channel_name"],
        )

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """
        Same as above but for uncached messages.
        """
        gid = getattr(payload, "guild_id", None)
        if not gid or not self._is_mapped_origin(gid):
            return

        if payload.cached_message is not None:
            return

        settings = self._settings_for_origin(gid)
        if not settings.get("ENABLE_CLONING", True) or not settings.get(
            "DELETE_MESSAGES", True
        ):
            return

        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(payload.channel_id)
            except Exception:
                return

        if isinstance(channel, discord.Thread):
            if not self.sitemap.in_scope_thread(channel):
                return
        else:
            if not self.sitemap.in_scope_channel(channel):
                return

        is_thread = getattr(channel, "type", None) in (
            ChannelType.public_thread,
            ChannelType.private_thread,
        )
        payload_out = {
            "type": "thread_message_delete" if is_thread else "message_delete",
            "data": {
                "guild_id": (
                    int(payload.guild_id) if payload.guild_id is not None else None
                ),
                "message_id": int(payload.message_id),
                "channel_id": int(payload.channel_id),
                "channel_name": getattr(channel, "name", str(payload.channel_id)),
                "channel_type": (
                    getattr(channel, "type", None).value
                    if getattr(channel, "type", None)
                    else None
                ),
                **(
                    {
                        "thread_parent_id": channel.parent.id,
                        "thread_parent_name": channel.parent.name,
                        "thread_id": channel.id,
                        "thread_name": channel.name,
                    }
                    if is_thread
                    else {}
                ),
            },
        }
        await self.ws.send(payload_out)
        logger.info(
            "[🗑️] Message delete detected in #%s → sent to server",
            payload_out["data"]["channel_name"],
        )

    async def on_thread_delete(self, thread: discord.Thread):
        """
        Event handler that is triggered when a thread is deleted in a Discord server.

        This method checks if the deleted thread belongs to the host guild. If it does,
        it sends a notification payload to the WebSocket server with the thread's ID.
        """
        g = getattr(thread, "guild", None)
        if not g or not self._is_mapped_origin(g.id):
            return
        settings = self._settings_for_origin(g.id)
        if not settings.get("ENABLE_CLONING", True):
            return

        if not self.sitemap.in_scope_thread(thread):
            logger.debug(
                "[thread] Ignoring delete for filtered-out thread %s (parent=%s)",
                getattr(thread, "id", None),
                getattr(getattr(thread, "parent", None), "id", None),
            )
            return
        payload = {
            "type": "thread_delete",
            "data": {"guild_id": thread.guild.id, "thread_id": thread.id},
        }
        await self.ws.send(payload)
        logger.info("[📩] Notified server of deleted thread %s", thread.id)

    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        """
        Handles updates to a Discord thread, such as renaming.
        """
        g = getattr(before, "guild", None)
        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(getattr(g, "id", None))

        if not settings.get("ENABLE_CLONING", True):
            return

        if not (
            self.sitemap.in_scope_thread(before) or self.sitemap.in_scope_thread(after)
        ):
            logger.debug(
                "[thread] Ignoring update for filtered-out thread %s (parent=%s)",
                getattr(before, "id", None),
                getattr(getattr(before, "parent", None), "id", None),
            )
            return

        if before.name != after.name:
            payload = {
                "type": "thread_rename",
                "data": {
                    "guild_id": before.guild.id,
                    "thread_id": before.id,
                    "new_name": after.name,
                    "old_name": before.name,
                    "parent_name": getattr(after.parent, "name", None),
                    "parent_id": getattr(after.parent, "id", None),
                },
            }
            logger.info(
                f"[✏️] Thread rename detected: {before.id} {before.name!r} → {after.name!r}"
            )
            await self.ws.send(payload)

    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        """
        Event handler that is triggered when a new channel is created in a guild.
        """
        g = getattr(channel, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(getattr(g, "id", None))

        if not settings.get("ENABLE_CLONING", True):
            return

        if not self.sitemap.in_scope_channel(channel):
            logger.debug(
                "Ignored create for filtered-out channel/category %s",
                getattr(channel, "id", None),
            )
            return
        self.schedule_sync(guild_id=g.id)

    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """
        Event handler that is triggered when a guild channel is deleted.
        """
        g = getattr(channel, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(channel.guild.id)

        if settings.get("ENABLE_CLONING", True):
            if not self.sitemap.in_scope_channel(channel):
                logger.debug(
                    "Ignored delete for filtered-out channel/category %s",
                    getattr(channel, "id", None),
                )
                return
            self.schedule_sync(guild_id=g.id)

    async def on_guild_channel_update(self, before, after):
        """
        Handles updates to guild channels within the host guild.
        This method is triggered when a guild channel is updated. It checks if the
        update occurred in the host guild and determines whether the update involves
        structural changes (such as a name change or a change in the parent category).
        If a structural change is detected, it schedules a synchronization process.
        """
        g = getattr(before, "guild", None)

        if not g or not self._is_mapped_origin(g.id):
            return
        settings = self._settings_for_origin(g.id)
        if not settings.get("ENABLE_CLONING", True):
            return

        perms_changed = False
        try:
            perms_changed = getattr(before, "overwrites", None) != getattr(
                after, "overwrites", None
            )
        except Exception:
            perms_changed = False

        if (
            settings.get("MIRROR_ROLE_PERMISSIONS", False)
            and settings.get("CLONE_ROLES", False)
            and perms_changed
        ):
            self.schedule_sync(guild_id=g.id)
            return

        if not (
            self.sitemap.in_scope_channel(before)
            or self.sitemap.in_scope_channel(after)
        ):
            logger.debug(
                "Ignored update for filtered-out channel/category %s",
                getattr(before, "id", None),
            )
            return
        name_changed = before.name != after.name
        parent_before = getattr(before, "category_id", None)
        parent_after = getattr(after, "category_id", None)
        parent_changed = parent_before != parent_after

        if name_changed or parent_changed:
            self.schedule_sync(guild_id=g.id)
        else:
            logger.debug(
                "Ignored channel update for %s: non-structural change", before.id
            )

    async def on_guild_role_create(self, role: discord.Role):

        g = getattr(role, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(getattr(g, "id", None))

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("CLONE_ROLES", True):
            return

        logger.debug("[roles] create: %s (%d) → scheduling sitemap", role.name, role.id)
        self.schedule_sync(guild_id=g.id)

    async def on_guild_role_delete(self, role: discord.Role):
        g = getattr(role, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(getattr(g, "id", None))

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("CLONE_ROLES", True):
            return

        logger.debug("[roles] delete: %s (%d) → scheduling sitemap", role.name, role.id)
        self.schedule_sync(guild_id=g.id)

    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        g = getattr(before, "guild", None)

        if not g or not self._is_mapped_origin(getattr(g, "id", None)):
            return

        settings = self._settings_for_origin(getattr(g, "id", None))

        if not settings.get("ENABLE_CLONING", True):
            return

        if not settings.get("CLONE_ROLES", True):
            return

        if not self.sitemap.role_change_is_relevant(before, after):
            logger.debug(
                "[roles] update ignored (irrelevant): %s (%d)", after.name, after.id
            )
            return
        logger.debug(
            "[roles] update: %s (%d) → scheduling sitemap", after.name, after.id
        )
        self.schedule_sync(guild_id=g.id)

    async def on_guild_join(self, guild: discord.Guild):
        try:
            row = self._guild_row_from_obj(guild)
            self.db.upsert_guild(**row)
            logger.debug("[guilds] join → upsert %s (%s)", guild.name, guild.id)
        except Exception:
            logger.exception("[guilds] on_guild_join failed")

    async def on_guild_remove(self, guild: discord.Guild):
        try:
            self.db.delete_guild(guild.id)
            logger.debug("[guilds] remove → delete %s", guild.id)
        except Exception:
            logger.exception("[guilds] on_guild_remove failed")

    async def on_guild_update(self, before: discord.Guild, after: discord.Guild):
        try:
            row = self._guild_row_from_obj(after)
            self.db.upsert_guild(**row)
            logger.debug("[guilds] update → upsert %s (%s)", after.name, after.id)
        except Exception:
            logger.exception("[guilds] on_guild_update failed")

    async def on_member_join(self, member: discord.Member):
        try:
            guild = member.guild

            if not self.db.get_onjoin_users(guild.id):
                return

            payload = {
                "type": "member_joined",
                "data": {
                    "guild_id": guild.id,
                    "guild_name": guild.name,
                    "user_id": member.id,
                    "username": str(member),
                    "display_name": getattr(member, "display_name", member.name),
                    "avatar_url": (
                        str(member.display_avatar.url)
                        if member.display_avatar
                        else None
                    ),
                    "joined_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            await self.ws.send(payload)
            logger.info(
                "[📩] Member join observed in %s: %s (%s) → notified server",
                guild.id,
                member.display_name,
                member.id,
            )

        except Exception:
            logger.exception("Failed to forward member_joined")

    async def on_guild_emojis_update(self, guild, before, after):
        if not self._is_mapped_origin(guild.id):
            return
        if resolve_mapping_settings(
            self.db, self.config, original_guild_id=guild.id
        ).get("CLONE_EMOJI", True):
            self.schedule_sync(guild_id=g.id)

    async def on_guild_stickers_update(self, guild, before, after):
        if not self._is_mapped_origin(guild.id):
            return
        if resolve_mapping_settings(
            self.db, self.config, original_guild_id=guild.id
        ).get("CLONE_STICKER", True):
            self.schedule_sync(guild_id=g.id)

    def _guild_row_from_obj(self, g: discord.Guild) -> dict:
        try:
            icon_url = str(g.icon.url) if getattr(g, "icon", None) else None
        except Exception:
            icon_url = None

        return {
            "guild_id": g.id,
            "name": g.name,
            "icon_url": icon_url,
            "owner_id": getattr(g, "owner_id", None),
            "member_count": getattr(g, "member_count", None),
            "description": getattr(g, "description", None),
        }

    async def _snapshot_all_guilds_once(self):
        try:
            current_ids = set()
            for g in list(self.bot.guilds):
                try:
                    current_ids.add(g.id)
                    row = self._guild_row_from_obj(g)
                    self.db.upsert_guild(**row)
                except Exception:
                    logger.exception(
                        "[guilds] snapshot: failed guild %s (%s)",
                        getattr(g, "name", "?"),
                        getattr(g, "id", "?"),
                    )

            known = set(self.db.get_all_guild_ids())
            stale = known - current_ids
            for gid in stale:
                self.db.delete_guild(gid)
        except Exception:
            logger.exception("[guilds] snapshot failed (outer)")

    async def _ensure_backfill_worker(self) -> None:
        if self._bf_worker_task and not self._bf_worker_task.done():
            return
        self._bf_worker_task = asyncio.create_task(self._backfill_worker(0))

    async def _backfill_worker(self, worker_id: int):
        while True:
            chan_id, params = await self._bf_queue.get()
            try:

                if chan_id in self._bf_active:
                    self._bf_queued.discard(chan_id)
                    continue

                self._bf_active.add(chan_id)

                await self.backfill.run_channel(chan_id, **(params or {}))

                await self.ws.send(
                    {
                        "type": "backfill_stream_end",
                        "data": {"channel_id": str(chan_id)},
                    }
                )

            except asyncio.CancelledError:

                raise
            except Exception:
                try:
                    logger.exception(
                        "[backfill] worker-%s failed for channel=%s", worker_id, chan_id
                    )
                except NameError:
                    import logging

                    logging.getLogger("backfill").exception(
                        "[backfill] worker-%s failed for channel=%s", worker_id, chan_id
                    )
            finally:
                self._bf_queued.discard(chan_id)
                self._bf_queue.task_done()

    async def _enqueue_backfill(self, chan_id: int, params: dict) -> dict:
        await self._ensure_backfill_worker()
        if chan_id in self._bf_active or chan_id in self._bf_queued:
            return {
                "ok": True,
                "queued": True,
                "position": None,
                "note": "already pending",
            }
        await self._bf_queue.put((chan_id, params))
        self._bf_queued.add(chan_id)
        pos = self._bf_queue.qsize()
        return {"ok": True, "queued": True, "position": pos}

    async def _shutdown(self):
        """
        Asynchronously shuts down the client.
        """
        logger.info("Shutting down client…")
        self.ws.begin_shutdown()
        self.bus.begin_shutdown()
        with contextlib.suppress(Exception):
            await self.ui_controller.stop()
        with contextlib.suppress(Exception, asyncio.TimeoutError):
            await asyncio.wait_for(
                self.bus.status(running=False, status="Stopped"), 0.4
            )
        try:
            t = getattr(self, "_scrape_task", None)
            if getattr(self, "scraper", None):
                self.scraper.request_cancel()
            if t and not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(t, timeout=5.0)
        except Exception as e:
            logger.debug("Shutdown error: %r", e)

        with contextlib.suppress(Exception):
            await self.bot.close()
        logger.info("Client shutdown complete.")

    def run(self):
        """
        Runs the Copycord client.
        """
        logger.info("[✨] Starting Copycord Client %s", CURRENT_VERSION)
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.bot.start(self.config.CLIENT_TOKEN))
        finally:
            loop.run_until_complete(self._shutdown())
            pending = asyncio.all_tasks(loop=loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()


def _autostart_enabled() -> bool:
    import os

    return os.getenv("COPYCORD_AUTOSTART", "true").lower() in ("1", "true", "yes", "on")


if __name__ == "__main__":
    if _autostart_enabled():
        ClientListener().run()
    else:
        import time

        while True:
            time.sleep(3600)
