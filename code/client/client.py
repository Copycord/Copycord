# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import contextlib
import json
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
import sys
from discord.ext import commands
from common.config import Config, CURRENT_VERSION
from common.db import DBManager
from client.sitemap import SitemapService
from client.message_utils import MessageUtils
from common.websockets import WebsocketManager, AdminBus
from client.scraper import MemberScraper
from client.helpers import ClientUiController


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
        self.host_guild_id = int(self.config.HOST_GUILD_ID)
        self.blocked_keywords = self.db.get_blocked_keywords()
        self._rebuild_blocklist(self.blocked_keywords)
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
        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)
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
            bot=self.bot,
            config=self.config,
            db=self.db,
            ws=self.ws,
            host_guild_id=self.host_guild_id,
            logger=logger,
        )
        self.ui_controller = ClientUiController(
            bus=self.bus,
            admin_base_url=self.config.ADMIN_WS_URL,
            bot=self.bot,
            guild_id=self.host_guild_id,
            listener=self,
            logger=logging.getLogger("client.ui"),
        )

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(self.bot.close())
            )

    async def _on_ws(self, msg: dict) -> dict | None:
        """
        Handles WebSocket (WS) messages received by the client.
        """
        typ = msg.get("type")
        data = msg.get("data", {})

        if typ == "settings_update":
            kws = data.get("blocked_keywords") or []
            self._rebuild_blocklist(kws)
            logger.info(
                "[⚙️] Updated block list: %d keywords", len(self.blocked_keywords)
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
                asyncio.create_task(self.sitemap.build_and_send())

            return {"ok": True, "note": "filters reloaded"}

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

            asyncio.create_task(
                self._backfill_channel(
                    chan_id,
                    after_iso=after_iso,
                    before_iso=before_iso,
                    last_n=last_n,
                )
            )
            return {"ok": True}

        elif typ == "sitemap_request":
            if self.config.ENABLE_CLONING:
                self.schedule_sync()
                logger.info("[🌐] Received sitemap request")
                return {"ok": True}
            else:
                return {"ok": False, "error": "Cloning is disabled"}

        elif typ == "scrape_members":
            data = data or {}

            inc_username = bool(data.get("include_username", False))
            inc_avatar_url = bool(data.get("include_avatar_url", False))
            inc_bio = bool(data.get("include_bio", False))
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

                        # Tell UI we're done
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
                            rgid = str(
                                self._scrape_gid
                                or gid
                                or self.host_guild_id
                                or "unknown"
                            )
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
            for src_id, row in self.chan_map.items():
                if int(row.get("cloned_channel_id") or 0) == channel_id:
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

            host_guild = None
            if hasattr(self, "host_guild_id") and self.host_guild_id:
                host_guild = self.bot.get_guild(int(self.host_guild_id))

            if host_guild is None and self.bot.guilds:
                host_guild = self.bot.guilds[0]
            guild = host_guild

        if guild is None:
            raise discord.Forbidden(
                None, {"message": "No guild available for fallback"}
            )

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
                await self.sitemap.build_and_send()
            except Exception:
                logger.exception("Error in periodic sync loop")
            await asyncio.sleep(self.config.SYNC_INTERVAL_SECONDS)

    def schedule_sync(self):
        self.sitemap.schedule_sync()

    async def on_ready(self):
        """
        Event handler that is triggered when the bot is ready.
        """
        host_guild = self.bot.get_guild(self.host_guild_id)
        if host_guild is None:
            logger.error(
                "[⛔] %s is not a member of the guild %s; shutting down.",
                self.bot.user,
                self.host_guild_id,
            )
            sys.exit(1)
        asyncio.create_task(self.config.setup_release_watcher(self, should_dm=False))
        msg = f"Logged in as {self.bot.user.display_name} in {host_guild.name}"
        self.ui_controller.start()
        await self.bus.status(running=True, status=msg, discord={"ready": True})
        logger.info("[🤖] %s", msg)
        if self.config.ENABLE_CLONING:
            if self._sync_task is None:
                self._sync_task = asyncio.create_task(self.periodic_sync_loop())
        else:
            logger.info("[🔕] Server cloning is disabled...")
        if self._ws_task is None:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))
        asyncio.create_task(self._snapshot_all_guilds_once())

    def _rebuild_blocklist(self, keywords: list[str] | None = None) -> None:
        if keywords is None:
            keywords = self.db.get_blocked_keywords()

        self.blocked_keywords = [
            k.lower().strip() for k in (keywords or []) if k and k.strip()
        ]

        self._blocked_patterns = [
            re.compile(rf"(?<!\w){re.escape(k)}(?!\w)", re.IGNORECASE)
            for k in self.blocked_keywords
        ]
        logger.debug("[⚙️] Block list now: %s", self.blocked_keywords)

    def should_ignore(self, message: discord.Message) -> bool:
        """
        Determines whether a given Discord message should be ignored based on various conditions.
        """
        ch = message.channel
        try:

            ch_id = getattr(ch, "id", None)
            cat_id = getattr(ch, "category_id", None)

            if (
                isinstance(getattr(ch, "__class__", None), type)
                and getattr(ch.__class__, "__name__", "") == "Thread"
            ):
                parent = getattr(ch, "parent", None)
                if parent is not None:

                    cat_id = getattr(parent, "category_id", cat_id)

            if self.sitemap.is_excluded_ids(ch_id, cat_id):
                return True
        except Exception:
            # Fail-safe: don't break message flow
            pass

        if message.type == MessageType.thread_created:
            return True

        if message.type == MessageType.channel_name_change:
            return True

        if message.guild is None or message.guild.id != self.host_guild_id:
            return True

        if message.channel.type in (ChannelType.voice, ChannelType.stage_voice):

            return True

        content = unicodedata.normalize("NFKC", message.content or "")
        for pat in getattr(self, "_blocked_patterns", []):
            if pat.search(content):
                logger.info(
                    "[❌] Dropping message %s: blocked keyword matched (%s)",
                    message.id,
                    pat.pattern,
                )
                return True

        return False

    async def maybe_send_announcement(self, message: discord.Message) -> bool:
        """
        Checks if a message contains any announcement triggers and sends an announcement
        if the conditions are met.
        """
        content = message.content
        lower = content.lower()
        author = message.author
        chan_id = message.channel.id

        triggers = self.db.get_announcement_triggers()

        for kw, entries in triggers.items():
            key = kw.lower()

            matched = False

            if re.match(r"^\w+$", key):
                if re.search(rf"\b{re.escape(key)}\b", lower):
                    matched = True

            if not matched and re.match(r"^[A-Za-z0-9_]+$", key):
                if re.search(rf"<a?:{re.escape(key)}:\d+>", content):
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
                            "keyword": kw,
                            "content": content,
                            "author": author.name,
                            "channel_id": chan_id,
                            "channel_name": message.channel.name,
                            "timestamp": str(message.created_at),
                        },
                    }
                    await self.ws.send(payload)
                    logger.info(f"[📢] Announcement `{kw}` by {author} (filtered).")
                    return True

        return False

    async def on_message(self, message: discord.Message):
        """
        Handles incoming Discord messages and processes them for forwarding.
        This method is triggered whenever a message is sent in a channel the bot has access to.
        """
        if self.config.ENABLE_CLONING:

            if self.should_ignore(message):
                return

            await self.maybe_send_announcement(message)

            raw = message.content or ""
            system = getattr(message, "system_content", "") or ""
            if not raw and system:
                content = system
                author = "System"
            else:
                content = raw
                author = message.author.name

            attachments = [
                {
                    "url": att.url,
                    "filename": att.filename,
                    "size": att.size,
                }
                for att in message.attachments
            ]

            raw_embeds = [e.to_dict() for e in message.embeds]
            mention_map = await self.msg.build_mention_map(message, raw_embeds)
            embeds = [
                self.msg.sanitize_embed_dict(e, message, mention_map)
                for e in raw_embeds
            ]
            content = self.msg.sanitize_inline(content, message, mention_map)

            components: list[dict] = []
            for comp in message.components:
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

            is_thread = message.channel.type in (
                ChannelType.public_thread,
                ChannelType.private_thread,
            )

            stickers_payload = self.msg.stickers_payload(
                getattr(message, "stickers", [])
            )

            payload = {
                "type": "thread_message" if is_thread else "message",
                "data": {
                    "channel_id": message.channel.id,
                    "channel_name": message.channel.name,
                    "channel_type": message.channel.type.value,
                    "author": author,
                    "author_id": message.author.id,
                    "avatar_url": (
                        str(message.author.display_avatar.url)
                        if message.author.display_avatar
                        else None
                    ),
                    "content": content,
                    "timestamp": str(message.created_at),
                    "attachments": attachments,
                    "components": components,
                    "stickers": stickers_payload,
                    "embeds": embeds,
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
                "[📩] New msg detected in #%s from %s; forwarding to server",
                message.channel.name,
                message.author.name,
            )

    async def on_thread_delete(self, thread: discord.Thread):
        """
        Event handler that is triggered when a thread is deleted in a Discord server.

        This method checks if the deleted thread belongs to the host guild. If it does,
        it sends a notification payload to the WebSocket server with the thread's ID.
        """
        if self.config.ENABLE_CLONING:
            if thread.guild.id != self.host_guild_id:
                return
            if not self.sitemap.in_scope_thread(thread):
                logger.debug(
                    "[thread] Ignoring delete for filtered-out thread %s (parent=%s)",
                    getattr(thread, "id", None),
                    getattr(getattr(thread, "parent", None), "id", None),
                )
                return
            payload = {"type": "thread_delete", "data": {"thread_id": thread.id}}
            await self.ws.send(payload)
            logger.info("[📩] Notified server of deleted thread %s", thread.id)

    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        """
        Handles updates to a Discord thread, such as renaming.
        """
        if not self.config.ENABLE_CLONING:
            return
        if not (before.guild and before.guild.id == self.host_guild_id):
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
        if self.config.ENABLE_CLONING:
            if channel.guild.id != self.host_guild_id:
                return

            if not self.sitemap.in_scope_channel(channel):
                logger.debug(
                    "Ignored create for filtered-out channel/category %s",
                    getattr(channel, "id", None),
                )
                return
            self.schedule_sync()

    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """
        Event handler that is triggered when a guild channel is deleted.
        """
        if self.config.ENABLE_CLONING:
            if channel.guild.id != self.host_guild_id:
                return
            if not self.sitemap.in_scope_channel(channel):
                logger.debug(
                    "Ignored delete for filtered-out channel/category %s",
                    getattr(channel, "id", None),
                )
                return
            self.schedule_sync()

    async def on_guild_channel_update(self, before, after):
        """
        Handles updates to guild channels within the host guild.
        This method is triggered when a guild channel is updated. It checks if the
        update occurred in the host guild and determines whether the update involves
        structural changes (such as a name change or a change in the parent category).
        If a structural change is detected, it schedules a synchronization process.
        """
        if self.config.ENABLE_CLONING:
            if before.guild.id != self.host_guild_id:
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
                self.schedule_sync()
            else:
                logger.debug(
                    "Ignored channel update for %s: non-structural change", before.id
                )

    async def on_guild_role_create(self, role: discord.Role):
        if not self.config.ENABLE_CLONING or not getattr(
            self.config, "CLONE_ROLES", True
        ):
            return
        if role.guild.id != self.host_guild_id:
            return
        logger.debug("[roles] create: %s (%d) → scheduling sitemap", role.name, role.id)
        self.schedule_sync()

    async def on_guild_role_delete(self, role: discord.Role):
        if not self.config.ENABLE_CLONING or not getattr(
            self.config, "CLONE_ROLES", True
        ):
            return
        if role.guild.id != self.host_guild_id:
            return
        logger.debug("[roles] delete: %s (%d) → scheduling sitemap", role.name, role.id)
        self.schedule_sync()

    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        if not self.config.ENABLE_CLONING or not getattr(
            self.config, "CLONE_ROLES", True
        ):
            return
        if after.guild.id != self.host_guild_id:
            return
        if not self.sitemap.role_change_is_relevant(before, after):
            logger.debug(
                "[roles] update ignored (irrelevant): %s (%d)", after.name, after.id
            )
            return
        logger.debug(
            "[roles] update: %s (%d) → scheduling sitemap", after.name, after.id
        )
        self.schedule_sync()

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

    async def _backfill_channel(
        self,
        original_channel_id: int,
        *,
        after_iso: str | None = None,
        before_iso: str | None = None,
        last_n: int | None = None,
    ):
        import asyncio
        import os
        from datetime import datetime, timezone

        def _coerce_int(x):
            try:
                return int(x)
            except Exception:
                return None

        try:
            from zoneinfo import ZoneInfo

            _DEFAULT_UI_TZ = os.getenv("UI_TZ", "America/New_York")
            LOCAL_TZ = ZoneInfo(_DEFAULT_UI_TZ)
        except Exception:
            LOCAL_TZ = datetime.now().astimezone().tzinfo or timezone.utc

        def _parse_iso(s: str | None) -> datetime | None:
            if not s:
                return None
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                try:
                    dt = datetime.fromisoformat(s + ":00")
                except Exception:
                    return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LOCAL_TZ).astimezone(timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt

        after_dt = _parse_iso(after_iso)
        before_dt = _parse_iso(before_iso)

        mode = (
            "last_n"
            if _coerce_int(last_n)
            else (
                "between"
                if (after_iso and before_iso)
                else ("since" if after_iso else "all")
            )
        )

        logger.debug(
            "[backfill] INIT | channel=%s mode=%s after_iso=%r before_iso=%r last_n=%r",
            original_channel_id,
            mode,
            after_iso,
            before_iso,
            last_n,
        )

        loop = asyncio.get_event_loop()
        t0 = loop.time()
        sent = 0
        skipped = 0
        last_ping = 0.0
        last_log = 0.0
        PROGRESS_EVERY = 50
        LOG_EVERY_SEC = 5.0

        await self.ws.send(
            {
                "type": "backfill_started",
                "data": {
                    "channel_id": original_channel_id,
                    "range": {
                        "after": after_iso,
                        "before": before_iso,
                        "last_n": last_n,
                    },
                },
            }
        )

        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            logger.error(
                "[backfill] ⛔ host guild missing | guild_id=%s", self.host_guild_id
            )
            try:
                await self.ws.send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "sent": sent},
                    }
                )
            finally:
                await self.ws.send(
                    {
                        "type": "backfill_stream_end",
                        "data": {"channel_id": original_channel_id},
                    }
                )
            return
        logger.debug(
            "[backfill] guild OK | id=%s name=%s",
            getattr(guild, "id", None),
            getattr(guild, "name", None),
        )

        ch = guild.get_channel(original_channel_id)
        if not ch:
            try:
                ch = await self.bot.fetch_channel(original_channel_id)
                logger.debug(
                    "[backfill] channel fetched | id=%s name=%s type=%s",
                    getattr(ch, "id", None),
                    getattr(ch, "name", None),
                    getattr(getattr(ch, "type", None), "value", None),
                )
            except Exception as e:
                logger.error(
                    "[backfill] ⛔ cannot fetch channel | id=%s err=%s",
                    original_channel_id,
                    e,
                )
                try:
                    await self.ws.send(
                        {
                            "type": "backfill_progress",
                            "data": {"channel_id": original_channel_id, "sent": sent},
                        }
                    )
                finally:
                    await self.ws.send(
                        {
                            "type": "backfill_stream_end",
                            "data": {"channel_id": original_channel_id},
                        }
                    )
                return
        else:
            logger.debug(
                "[backfill] channel OK | id=%s name=%s type=%s",
                getattr(ch, "id", None),
                getattr(ch, "name", None),
                getattr(getattr(ch, "type", None), "value", None),
            )

        if getattr(getattr(ch, "guild", None), "id", None) != self.host_guild_id:
            logger.warning(
                "[backfill] channel is not in host guild | got_guild=%s expected=%s (was a clone id passed?)",
                getattr(getattr(ch, "guild", None), "id", None),
                self.host_guild_id,
            )

        ALLOWED_TYPES = {MessageType.default}
        for _name in ("reply", "application_command", "context_menu_command"):
            _t = getattr(MessageType, _name, None)
            if _t is not None:
                ALLOWED_TYPES.add(_t)
        logger.debug(
            "[backfill] allowed message types: %s",
            sorted(getattr(t, "value", str(t)) for t in ALLOWED_TYPES),
        )

        def _is_normal(msg) -> bool:
            try:
                if callable(getattr(msg, "is_system", None)) and msg.is_system():
                    return False
            except Exception:
                pass
            if getattr(getattr(msg, "author", None), "system", False):
                return False
            if getattr(msg, "type", None) not in ALLOWED_TYPES:
                return False
            return True

        if after_iso and after_dt:
            logger.debug(
                "[backfill] since filter parsed | utc=%s", after_dt.isoformat()
            )
        elif after_iso and not after_dt:
            logger.warning(
                "[backfill] since filter parse failed | after_iso=%r (ignoring)",
                after_iso,
            )

        if before_iso and before_dt:
            logger.debug(
                "[backfill] before filter parsed | utc=%s", before_dt.isoformat()
            )
        elif before_iso and not before_dt:
            logger.warning(
                "[backfill] before filter parse failed | before_iso=%r (ignoring)",
                before_iso,
            )

        try:

            async def emit_msg(m):
                nonlocal sent, skipped, last_ping, last_log
                if not _is_normal(m):
                    skipped += 1
                    return

                raw = m.content or ""
                system = getattr(m, "system_content", "") or ""
                content = system if (not raw and system) else raw
                author = (
                    "System"
                    if (not raw and system)
                    else getattr(m.author, "name", "Unknown")
                )

                raw_embeds = [e.to_dict() for e in m.embeds]
                mention_map = await self.msg.build_mention_map(m, raw_embeds)
                embeds = [
                    self.msg.sanitize_embed_dict(e, m, mention_map) for e in raw_embeds
                ]
                content = self.msg.sanitize_inline(content, m, mention_map)
                stickers_payload = self.msg.stickers_payload(getattr(m, "stickers", []))

                await self.ws.send(
                    {
                        "type": "message",
                        "data": {
                            "channel_id": m.channel.id,
                            "channel_name": getattr(m.channel, "name", None),
                            "channel_type": (
                                getattr(m.channel, "type", None).value
                                if getattr(m.channel, "type", None)
                                else None
                            ),
                            "author": author,
                            "author_id": getattr(m, "author", None)
                            and getattr(m.author, "id", None),
                            "avatar_url": (
                                str(m.author.display_avatar.url)
                                if getattr(m.author, "display_avatar", None)
                                else None
                            ),
                            "content": content,
                            "embeds": embeds,
                            "attachments": [
                                {"url": a.url, "filename": a.filename, "size": a.size}
                                for a in m.attachments
                            ],
                            "stickers": stickers_payload,
                            "__backfill__": True,
                        },
                    }
                )
                sent += 1
                await asyncio.sleep(2)

                now = loop.time()
                if sent % PROGRESS_EVERY == 0 or (now - last_ping) >= 2.0:
                    await self.ws.send(
                        {
                            "type": "backfill_progress",
                            "data": {"channel_id": original_channel_id, "sent": sent},
                        }
                    )
                    last_ping = now

                if (now - last_log) >= LOG_EVERY_SEC:
                    dt = now - t0
                    rate = (sent / dt) if dt > 0 else 0.0
                    logger.debug(
                        "[backfill] progress | channel=%s sent=%d skipped=%d rate=%.1f msg/s",
                        original_channel_id,
                        sent,
                        skipped,
                        rate,
                    )
                    last_log = now

            n = _coerce_int(last_n)

            if n and n > 0:

                buf = []
                logger.debug(
                    "[backfill] mode=last_n | n=%d %s",
                    n,
                    (
                        f"(after {after_dt.isoformat()})"
                        if after_dt
                        else "(no since bound)"
                    ),
                )
                async for m in ch.history(
                    limit=n, oldest_first=False, after=after_dt, before=before_dt
                ):
                    buf.append(m)

                total = sum(1 for m in buf if _is_normal(m))
                logger.debug(
                    "[backfill] fetched buffer | size=%d, total_normal=%d",
                    len(buf),
                    total,
                )

                await self.ws.send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "total": total},
                    }
                )

                for m in reversed(buf):
                    await emit_msg(m)

            else:

                logger.debug(
                    "[backfill] mode=%s | streaming oldest→newest%s",
                    "since" if after_dt else "all",
                    f" (after {after_dt.isoformat()})" if after_dt else "",
                )
                buf = [
                    m
                    async for m in ch.history(
                        limit=None, oldest_first=True, after=after_dt, before=before_dt
                    )
                ]
                total = sum(1 for m in buf if _is_normal(m))
                logger.debug(
                    "[backfill] pre-count complete | fetched=%d total_normal=%d",
                    len(buf),
                    total,
                )

                await self.ws.send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "total": total},
                    }
                )

                for m in buf:
                    await emit_msg(m)

        except Forbidden as e:
            logger.debug(
                "[backfill] history forbidden | channel=%s err=%s",
                original_channel_id,
                e,
            )
        except HTTPException as e:
            logger.warning(
                "[backfill] HTTP error | channel=%s err=%s", original_channel_id, e
            )
        except Exception as e:
            logger.exception(
                "[backfill] unexpected error | channel=%s err=%s",
                original_channel_id,
                e,
            )
        finally:
            try:
                await self.ws.send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "sent": sent},
                    }
                )
            except Exception:
                pass

            dur = loop.time() - t0
            logger.debug(
                "[backfill] STREAM END | channel=%s mode=%s sent=%d skipped=%d dur=%.1fs",
                original_channel_id,
                mode,
                sent,
                skipped,
                dur,
            )
            await self.ws.send(
                {
                    "type": "backfill_stream_end",
                    "data": {"channel_id": original_channel_id},
                }
            )

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

        This method initializes the asyncio event loop and starts the bot using the
        provided client token from the configuration. It ensures proper shutdown
        of the bot and cleanup of pending tasks when the event loop is closed.
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
