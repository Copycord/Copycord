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
import re
import signal
import traceback
import unicodedata
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional
import discord
from discord import ChannelType, MessageType, Member
from discord.errors import Forbidden, HTTPException
import os
import pprint
import sys
from discord.ext import commands
from common.config import Config
from common.db import DBManager
from common.websockets import WebsocketManager
from client.scraper import MemberScraper
from client.scraper import StreamManager


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

log_file = os.path.join(LOG_DIR, "client.log")
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
        self._sync_task: Optional[asyncio.Task] = None
        self.debounce_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._m_user = re.compile(r"<@!?(\d+)>")
        self.scraper = getattr(self, "scraper", None)
        self._scrape_lock = getattr(self, "_scrape_lock", asyncio.Lock())
        self.streams = getattr(self, "streams", StreamManager(logger))
        self._scrape_task: asyncio.Task | None = None
        self._last_cancel_at: float | None = None
        self._cancelling: bool = False
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
        self.ws = WebsocketManager(
            send_url=self.config.SERVER_WS_URL,
            listen_host=self.config.CLIENT_WS_HOST,
            listen_port=self.config.CLIENT_WS_PORT,
            logger=logger,
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
            return  # optional ack if your WS expects it

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

        elif typ == "clone_messages":
            chan_id = int(data.get("channel_id"))
            asyncio.create_task(self._backfill_channel(chan_id))
            return {"ok": True}

        elif typ == "sitemap_request":
            if self.config.ENABLE_CLONING:
                self.schedule_sync()
                logger.info("[🌐] Received sitemap request")
                return {"ok": True}
            else:
                return {"ok": False, "error": "Cloning is disabled"}

        elif typ == "scrape_members":
            include_names = bool(data.get("include_names", True))

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
                mpps = clamp(int(mpps), 1, 5)

            try:
                if self.scraper is None:
                    self.scraper = MemberScraper(self.bot, self.config, logger=logger)

                async with self._scrape_lock:
                    if self._scrape_task and not self._scrape_task.done():
                        logger.debug("[scrape] REJECT: already-running task")
                        return {"ok": False, "error": "scrape-already-running"}

                    self._scrape_task = asyncio.create_task(
                        self.scraper.scrape(
                            include_names=include_names,
                            num_sessions=ns,
                            max_parallel_per_session=mpps,
                        ),
                        name="scrape",
                    )
                    try:
                        result = await self._scrape_task
                        logger.debug(
                            "[scrape] TASK_AWAITED_OK count=%d",
                            len((result or {}).get("members", [])),
                        )
                        return {"ok": True, "data": result}
                    except asyncio.CancelledError:
                        logger.debug("[scrape] TASK_AWAITED_CANCELLED")
                        return {"ok": False, "error": "cancelled"}
                    except BaseException as e:
                        logger.exception("[❌] OP8 scrape failed: %r", e)
                        return {"ok": False, "error": str(e)}
                    finally:
                        self._scrape_task = None
            except BaseException as e:
                logger.exception("[❌] OP8 scrape failed (outer): %r", e)
                return {"ok": False, "error": str(e)}

        elif typ == "stream_next":
            sid = data.get("id")
            offset = int(data.get("offset", 0))
            length = int(data.get("length", 0) or 0)
            return self.streams.next(sid, offset, length or None)

        elif typ == "stream_abort":
            return self.streams.abort(data.get("id"))

        elif typ == "scrape_cancel":
            try:
                self._cancelling = True
                t = getattr(self, "_scrape_task", None)
                if getattr(self, "scraper", None):
                    self.scraper.request_cancel()
                    logger.debug("[scrape] CANCEL → cooperative flag set")
                if t and not t.done():
                    t.cancel()
                    logger.debug("[scrape] CANCEL → hard cancel issued to task")
                return {"ok": True}
            except Exception as e:
                logger.exception("[scrape] CANCEL failed")
                return {"ok": False, "error": str(e)}

        elif typ == "scrape_snapshot":
            try:
                include_names = bool((data or {}).get("include_names", True))
                if getattr(self, "scraper", None):
                    members = await self.scraper.snapshot_members()
                    return self.streams.pack_json(
                        {"members": members, "count": len(members)},
                        max_inline_bytes=800_000,
                    )
                return self.streams.pack_json(
                    {"members": [], "count": 0}, max_inline_bytes=800_000
                )
            except Exception as e:
                logger.exception("[❌] scrape_snapshot failed")
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

        # 1) Map cloned -> host channel id BEFORE any fetches
        channel_id = int(orig_channel_id)
        if hasattr(self, "chan_map"):
            for src_id, row in self.chan_map.items():
                if int(row.get("cloned_channel_id") or 0) == channel_id:
                    logger.debug(
                        f"[map] Mapped cloned channel {channel_id} -> host channel {src_id}"
                    )
                    channel_id = int(src_id)
                    break

        # 2) Try cache, then fetch
        channel = self.bot.get_channel(channel_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except discord.Forbidden:
                channel = None

        # 3) Resolve guild
        guild = getattr(channel, "guild", None)
        if guild is None:
            # Prefer an explicit host guild id if your client has it
            host_guild = None
            if hasattr(self, "host_guild_id") and self.host_guild_id:
                host_guild = self.bot.get_guild(int(self.host_guild_id))
            # Fallback: try to guess (last resort)
            if host_guild is None and self.bot.guilds:
                host_guild = self.bot.guilds[0]
            guild = host_guild

        if guild is None:
            raise discord.Forbidden(
                None, {"message": "No guild available for fallback"}
            )

        # 4) If we couldn’t fetch the requested channel (or it’s None), pick the first readable text channel
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

    async def build_and_send_sitemap(self):
        """
        Asynchronously builds and sends a sitemap of the Discord guild to the server.
        The sitemap includes information about categories, standalone text channels, forums, threads, emojis,
        stickers, and community settings of the guild. It fetches additional thread data from the database and ensures
        all relevant information is sent to the server via a WebSocket connection.
        """
        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            logger.error("[⛔] Guild %s not found", self.host_guild_id)
            return

        def _enum_int(val, default=0):
            if val is None:
                return default
            v = getattr(val, "value", val)
            try:
                return int(v)
            except Exception:
                return default

        def _sticker_url(s):
            u = getattr(s, "url", None)
            if not u:
                asset = getattr(s, "asset", None)
                u = getattr(asset, "url", None) if asset else None
            return str(u) if u else ""

        try:
            fetched_stickers = await guild.fetch_stickers()
        except Exception as e:
            logger.warning("[🎟️] Could not fetch stickers: %s", e)
            fetched_stickers = list(getattr(guild, "stickers", []))

        sitemap = {
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": [
                {"id": e.id, "name": e.name, "url": str(e.url), "animated": e.animated}
                for e in guild.emojis
            ],
            "stickers": [],
            "roles": [],
            "community": {
                "enabled": "COMMUNITY" in guild.features,
                "rules_channel_id": (
                    guild.rules_channel.id if guild.rules_channel else None
                ),
                "public_updates_channel_id": (
                    guild.public_updates_channel.id
                    if guild.public_updates_channel
                    else None
                ),
            },
        }

        try:
            guild_sticker_type_val = getattr(discord.StickerType, "guild").value
        except Exception:
            guild_sticker_type_val = 1

        stickers_payload = []
        for s in fetched_stickers:
            stype = _enum_int(getattr(s, "type", None), default=guild_sticker_type_val)
            if stype != guild_sticker_type_val:
                continue

            stickers_payload.append(
                {
                    "id": s.id,
                    "name": s.name,
                    "format_type": _enum_int(
                        getattr(s, "format", None) or getattr(s, "format_type", None), 0
                    ),
                    "url": _sticker_url(s),
                    "tags": getattr(s, "tags", "") or "",
                    "description": getattr(s, "description", "") or "",
                    "available": bool(getattr(s, "available", True)),
                }
            )
        sitemap["stickers"] = stickers_payload

        sitemap["roles"] = [
            {
                "id": r.id,
                "name": r.name,
                "permissions": r.permissions.value,
                "color": r.color.value if hasattr(r.color, "value") else int(r.color),
                "hoist": r.hoist,
                "mentionable": r.mentionable,
                "managed": r.managed,
                "everyone": (r == r.guild.default_role),
                "position": r.position,
            }
            for r in guild.roles
        ]

        for cat in guild.categories:
            channels = [
                {"id": ch.id, "name": ch.name, "type": ch.type.value}
                for ch in cat.channels
                if isinstance(ch, discord.TextChannel)
            ]
            sitemap["categories"].append(
                {"id": cat.id, "name": cat.name, "channels": channels}
            )

        sitemap["standalone_channels"] = [
            {"id": ch.id, "name": ch.name, "type": ch.type.value}
            for ch in guild.text_channels
            if ch.category is None
        ]

        for forum in getattr(guild, "forums", []):
            sitemap["forums"].append(
                {
                    "id": forum.id,
                    "name": forum.name,
                    "category_id": forum.category.id if forum.category else None,
                }
            )

        seen = {t["id"] for t in sitemap["threads"]}
        for row in self.db.get_all_threads():
            orig_tid = row["original_thread_id"]
            forum_orig = row["forum_original_id"]

            if orig_tid in seen:
                continue

            thr = guild.get_channel(orig_tid)
            if not thr:
                try:
                    thr = await self.bot.fetch_channel(orig_tid)
                except Exception:
                    continue

            if not isinstance(thr, discord.Thread):
                continue

            sitemap["threads"].append(
                {
                    "id": thr.id,
                    "forum_id": forum_orig,
                    "name": thr.name,
                    "archived": thr.archived,
                }
            )

        sitemap = self._filter_sitemap(sitemap)
        await self.ws.send({"type": "sitemap", "data": sitemap})
        logger.info("[📩] Sitemap sent to Server")

    async def periodic_sync_loop(self):
        """
        Periodically synchronizes data by building and sending a sitemap.
        """
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        while True:
            try:
                await self.build_and_send_sitemap()
            except Exception:
                logger.exception("Error in periodic sync loop")
            await asyncio.sleep(self.config.SYNC_INTERVAL_SECONDS)

    async def _debounced_sitemap(self):
        """
        An asynchronous helper method that ensures the sitemap is built and sent
        after a short delay, while preventing multiple concurrent executions.
        """
        try:
            await asyncio.sleep(1)
            await self.build_and_send_sitemap()
        finally:
            self.debounce_task = None

    def schedule_sync(self):
        """
        Schedules a debounced synchronization task.

        This method checks if a debounce task is already scheduled. If not, it creates
        and schedules a new asynchronous task to perform a debounced sitemap synchronization.
        """
        if self.debounce_task is None:
            self.debounce_task = asyncio.create_task(self._debounced_sitemap())

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
        logger.info("[🤖] Logged in as %s in guild %s", self.bot.user, host_guild.name)
        if self.config.ENABLE_CLONING:
            if self._sync_task is None:
                self._sync_task = asyncio.create_task(self.periodic_sync_loop())
        else:
            logger.info("[🔕] Server cloning is disabled...")
        if self._ws_task is None:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))

    def extract_public_message_attrs(self, message: discord.Message) -> dict:
        """
        Extracts public (non-private and non-callable) attributes from a discord.Message object.
        """
        attrs = {}
        for name in dir(message):
            if name.startswith("_"):
                continue

            try:
                value = getattr(message, name)
            except Exception:
                continue

            if callable(value):
                continue

            attrs[name] = value
        return attrs

    def _rebuild_blocklist(self, keywords: list[str] | None = None) -> None:
        if keywords is None:
            keywords = self.db.get_blocked_keywords()
        # normalize and store canonical list
        self.blocked_keywords = [
            k.lower().strip() for k in (keywords or []) if k and k.strip()
        ]
        # word-boundary patterns; matches "yo" but not "you"
        self._blocked_patterns = [
            re.compile(rf"(?<!\w){re.escape(k)}(?!\w)", re.IGNORECASE)
            for k in self.blocked_keywords
        ]
        logger.debug("[⚙️] Block list now: %s", self.blocked_keywords)

    def _filter_sitemap(self, sitemap: dict) -> dict:
        wl_on = bool(self.config.whitelist_enabled)
        inc_cats = getattr(self.config, "include_category_ids", set())

        new_categories = []
        for cat in sitemap.get("categories", []):
            cat_id = int(cat["id"])

            # Skip whole category if excluded (or not whitelisted when WL is ON)
            if self._is_filtered_out(None, cat_id):
                continue

            kept_children = [
                ch
                for ch in cat.get("channels", [])
                if not self._is_filtered_out(int(ch["id"]), cat_id)
            ]

            keep_empty = (not wl_on) or (cat_id in inc_cats)
            if kept_children or keep_empty:
                new_categories.append({**cat, "channels": kept_children})

        standalones = [
            ch
            for ch in sitemap.get("standalone_channels", [])
            if not self._is_filtered_out(int(ch["id"]), None)
        ]

        forums = [
            f
            for f in sitemap.get("forums", [])
            if not self._is_filtered_out(int(f["id"]), int(f.get("category_id") or 0))
        ]

        keep_forum_ids = {int(f["id"]) for f in forums}
        threads = [
            t
            for t in sitemap.get("threads", [])
            if int(t.get("forum_id", 0)) in keep_forum_ids
        ]

        out = dict(sitemap)
        out["categories"] = new_categories
        out["standalone_channels"] = standalones
        out["forums"] = forums
        out["threads"] = threads
        return out

    def _is_filtered_out(self, channel_id: int | None, category_id: int | None) -> bool:
        """
        Return True if this (channel, category) should be dropped (ignored).
        Behavior:
        - If whitelist is enabled, only IDs present in the whitelist are allowed.
        - Excludes still apply, but whitelist wins on conflicts.
        """
        cfg = self.config

        # 1) Whitelist gate (if enabled)
        if cfg.whitelist_enabled:
            allowed = False
            if channel_id and channel_id in cfg.include_channel_ids:
                allowed = True
            elif category_id and category_id in cfg.include_category_ids:
                allowed = True
            if not allowed:
                return True  # filtered out

        # 2) Exclude lists (whitelist wins)
        if channel_id and channel_id in cfg.excluded_channel_ids:
            # Explicit channel whitelist overrides channel exclude
            if channel_id in cfg.include_channel_ids:
                return False
            # Category whitelist also overrides
            if category_id and category_id in cfg.include_category_ids:
                return False
            return True

        if category_id and category_id in cfg.excluded_category_ids:
            # Category/channel explicit whitelists override
            if category_id in cfg.include_category_ids:
                return False
            if channel_id and channel_id in cfg.include_channel_ids:
                return False
            return True

        return False

    # Backward-compat wrapper (optional):
    def _is_excluded_ids(self, channel_id: int | None, category_id: int | None) -> bool:
        return self._is_filtered_out(channel_id, category_id)

    def should_ignore(self, message: discord.Message) -> bool:
        """
        Determines whether a given Discord message should be ignored based on various conditions.
        """
        ch = message.channel
        try:
            # For text channels
            ch_id = getattr(ch, "id", None)
            cat_id = getattr(ch, "category_id", None)

            # For threads: category_id lives on the parent forum
            if (
                isinstance(getattr(ch, "__class__", None), type)
                and getattr(ch.__class__, "__name__", "") == "Thread"
            ):
                parent = getattr(ch, "parent", None)
                if parent is not None:
                    # parent is a ForumChannel (has its own category_id)
                    cat_id = getattr(parent, "category_id", cat_id)

            if self._is_filtered_out(ch_id, cat_id):
                return True
        except Exception:
            # Fail-safe: don't break message flow
            pass

        # Ignore thread_created events in text channels
        if message.type == MessageType.thread_created:
            return True

        # Ignore channel name change system messages in threads
        if message.type == MessageType.channel_name_change:
            return True

        # Ignore DMs or wrong guild
        if message.guild is None or message.guild.id != self.host_guild_id:
            return True

        if message.channel.type in (ChannelType.voice, ChannelType.stage_voice):
            # This is a voice channel text chat; unsupported → skip.
            return True

        # Ignore blocked keywords
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
            # try word-boundary match for simple alphanumerics
            if re.match(r"^\w+$", key):
                if re.search(rf"\b{re.escape(key)}\b", lower):
                    matched = True
            # try custom-emoji markup: <:name:digits> or <a:name:digits>
            if not matched and re.match(r"^[A-Za-z0-9_]+$", key):
                if re.search(rf"<a?:{re.escape(key)}:\d+>", content):
                    matched = True
            # fallback: substring (for Unicode emoji, punctuation, spaces, etc)
            if not matched and key in lower:
                matched = True

            if not matched:
                continue

            # check filters
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

    def _humanize_user_mentions(
        self,
        content: str,
        message: discord.Message,
        id_to_name_override: dict[str, str] | None = None,
    ) -> str:
        if not content:
            return content

        id_to_name = dict(id_to_name_override or {})

        # seed with message.mentions (helps for content)
        for m in getattr(message, "mentions", []):
            name = f"@{(m.display_name if isinstance(m, Member) else m.name) or m.name}"
            id_to_name[str(m.id)] = name

        def repl(match: re.Match) -> str:
            uid = match.group(1)
            name = id_to_name.get(uid)
            if name:
                return name
            g = message.guild
            mem = g.get_member(int(uid)) if g else None
            if mem:
                nm = f"@{mem.display_name or mem.name}"
                id_to_name[uid] = nm
                return nm
            return match.group(0)

        return self._m_user.sub(repl, content)

    def _sanitize_inline(self, s, message=None, id_map=None):
        if not s:
            return s
        # Replace {mention} with actual mention text
        if "{mention}" in s:
            s = s.replace("{mention}", f"@{message.author.display_name}")
        if message:
            s = self._humanize_user_mentions(s, message, id_map)
        return s

    def _sanitize_embed_dict(
        self,
        d: dict,
        message: discord.Message,
        id_map: dict[str, str] | None = None,
    ) -> dict:
        e = dict(d)

        # top-level
        if "title" in e:
            e["title"] = self._sanitize_inline(e.get("title"), message, id_map)
        if "description" in e:
            e["description"] = self._sanitize_inline(
                e.get("description"), message, id_map
            )

        # author
        if isinstance(e.get("author"), dict) and "name" in e["author"]:
            e["author"] = dict(e["author"])
            e["author"]["name"] = self._sanitize_inline(
                e["author"].get("name"), message, id_map
            )

        # footer
        if isinstance(e.get("footer"), dict) and "text" in e["footer"]:
            e["footer"] = dict(e["footer"])
            e["footer"]["text"] = self._sanitize_inline(
                e["footer"].get("text"), message, id_map
            )

        # fields
        if isinstance(e.get("fields"), list):
            new_fields = []
            for f in e["fields"]:
                if not isinstance(f, dict):
                    new_fields.append(f)
                    continue
                f2 = dict(f)
                if "name" in f2:
                    f2["name"] = self._sanitize_inline(f2.get("name"), message, id_map)
                if "value" in f2:
                    f2["value"] = self._sanitize_inline(
                        f2.get("value"), message, id_map
                    )
                new_fields.append(f2)
            e["fields"] = new_fields

        return e

    async def _build_mention_map(
        self, message: discord.Message, embed_dicts: list[dict]
    ) -> dict[str, str]:
        ids: set[str] = set()

        def _collect(s: str | None):
            if not s:
                return
            ids.update(self._m_user.findall(s))

        # collect from content
        _collect(message.content)

        # collect from embeds
        for e in embed_dicts:
            _collect(e.get("title"))
            _collect(e.get("description"))
            a = e.get("author") or {}
            _collect(a.get("name"))
            f = e.get("footer") or {}
            _collect(f.get("text"))
            for fld in e.get("fields") or []:
                _collect(fld.get("name"))
                _collect(fld.get("value"))

        if not ids:
            return {}

        g = message.guild
        id_to_name: dict[str, str] = {}

        for sid in ids:
            uid = int(sid)
            # 1) cache
            mem = g.get_member(uid) if g else None
            if mem:
                id_to_name[sid] = f"@{mem.display_name or mem.name}"
                continue
            # 2) fetch member for display name
            try:
                if g:
                    mem = await g.fetch_member(uid)
                    id_to_name[sid] = f"@{mem.display_name or mem.name}"
                    continue
            except Exception:
                pass
            # fallback: global user (no guild display name)
            try:
                u = await self.bot.fetch_user(uid)
                id_to_name[sid] = f"@{u.name}"
            except Exception:
                # leave unresolved; replacer will keep original token
                pass

        return id_to_name

    async def on_message(self, message: discord.Message):
        """
        Handles incoming Discord messages and processes them for forwarding.
        This method is triggered whenever a message is sent in a channel the bot has access to.
        """
        if self.config.ENABLE_CLONING:

            if self.should_ignore(message):
                return

            await self.maybe_send_announcement(message)

            # Normalize content and detect system messages
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
            mention_map = await self._build_mention_map(message, raw_embeds)
            embeds = [
                self._sanitize_embed_dict(e, message, mention_map) for e in raw_embeds
            ]
            content = self._sanitize_inline(content, message, mention_map)

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

            def _enum_int(val, default=0):
                v = getattr(val, "value", val)
                try:
                    return int(v)
                except Exception:
                    return default

            def _sticker_url(s):
                u = getattr(s, "url", None)
                if not u:
                    asset = getattr(s, "asset", None)
                    u = getattr(asset, "url", None) if asset else None
                return str(u) if u else ""

            stickers_payload = []
            for s in getattr(message, "stickers", []) or []:
                stickers_payload.append(
                    {
                        "id": s.id,
                        "name": s.name,
                        "format_type": _enum_int(getattr(s, "format", None), 0),
                        "url": _sticker_url(s),  # <-- add this
                    }
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

            # Pull message attributes for debugging
            # msg_attrs = self.extract_public_message_attrs(message)

            # logger.debug(
            #     "Full Message attributes:\n%s",
            #     pprint.pformat(msg_attrs, indent=2, width=120),
            # )

    def _role_change_is_relevant(
        self, before: discord.Role, after: discord.Role
    ) -> bool:
        """
        Only schedule a sitemap when something Copycord can mirror actually changed.
        Skip @everyone and managed roles. (Role position changes are ignored.)
        """
        try:
            # Skip noise
            if after.is_default() or after.managed:
                return False

            # Name / perms / color / hoist / mentionable  (INTENTIONALLY ignore position)
            if before.name != after.name:
                return True

            if getattr(before.permissions, "value", 0) != getattr(
                after.permissions, "value", 0
            ):
                return True

            def _colval(c):
                try:
                    return c.value
                except Exception:
                    return int(c)

            if _colval(before.color) != _colval(after.color):
                return True

            if before.hoist != after.hoist:
                return True

            if before.mentionable != after.mentionable:
                return True

        except Exception:
            return True

        return False

    def _in_scope_channel(self, ch) -> bool:
        """
        Returns True if changes to `ch` should affect the filtered sitemap.
        - CategoryChannel: test by category-id only
        - Thread: test via parent channel + its category
        - Text/Forum channel: test via channel-id + its category
        """
        try:
            if isinstance(ch, discord.CategoryChannel):
                return not self._is_filtered_out(None, ch.id)

            # For threads, check the parent channel instead
            if isinstance(ch, discord.Thread):
                parent = getattr(ch, "parent", None)
                if parent is None:
                    return False
                cat_id = getattr(parent, "category_id", None)
                return not self._is_filtered_out(parent.id, cat_id)

            # Text / Forum channels
            cat_id = getattr(ch, "category_id", None)
            return not self._is_filtered_out(getattr(ch, "id", None), cat_id)
        except Exception:
            return True

    def _in_scope_thread(self, thr: discord.Thread) -> bool:
        """
        Returns True if this thread should affect the filtered sitemap / message flow.
        We gate by the PARENT channel + its category (we don't whitelist/exclude by thread ID).
        """
        try:
            parent = getattr(thr, "parent", None)
            if parent is None:
                return False
            cat_id = getattr(parent, "category_id", None)
            return not self._is_filtered_out(getattr(parent, "id", None), cat_id)
        except Exception:
            return True

    async def on_thread_delete(self, thread: discord.Thread):
        """
        Event handler that is triggered when a thread is deleted in a Discord server.

        This method checks if the deleted thread belongs to the host guild. If it does,
        it sends a notification payload to the WebSocket server with the thread's ID.
        """
        if self.config.ENABLE_CLONING:
            if thread.guild.id != self.host_guild_id:
                return
            if not self._in_scope_thread(thread):
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

        if not (self._in_scope_thread(before) or self._in_scope_thread(after)):
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

            if not self._in_scope_channel(channel):
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
            if not self._in_scope_channel(channel):
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

            if not (self._in_scope_channel(before) or self._in_scope_channel(after)):
                logger.debug(
                    "Ignored update for filtered-out channel/category %s",
                    getattr(before, "id", None),
                )
                return

            # Only resync if name or parent category changed
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
        if not self._role_change_is_relevant(before, after):
            logger.debug(
                "[roles] update ignored (irrelevant): %s (%d)", after.name, after.id
            )
            return
        logger.debug(
            "[roles] update: %s (%d) → scheduling sitemap", after.name, after.id
        )
        self.schedule_sync()

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

    async def _backfill_channel(self, original_channel_id: int):
        """Grab all human/bot messages from a channel and send to server"""
        await self.ws.send(
            {"type": "backfill_started", "data": {"channel_id": original_channel_id}}
        )

        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            logger.error("[⛔] Host guild %s not available", self.host_guild_id)
            await self.ws.send(
                {"type": "backfill_done", "data": {"channel_id": original_channel_id}}
            )
            return

        ch = guild.get_channel(original_channel_id)
        if not ch:
            try:
                ch = await self.bot.fetch_channel(original_channel_id)
            except Exception as e:
                logger.error("[⛔] Cannot fetch channel %s: %s", original_channel_id, e)
                await self.ws.send(
                    {
                        "type": "backfill_done",
                        "data": {"channel_id": original_channel_id},
                    }
                )
                return

        # ----- filter: only human/bot messages, exclude all system messages -----
        ALLOWED_TYPES = {MessageType.default}
        for _name in ("reply", "application_command", "context_menu_command"):
            _t = getattr(MessageType, _name, None)
            if _t is not None:
                ALLOWED_TYPES.add(_t)

        def _is_normal(msg) -> bool:
            # skip if Discord flags it as system
            try:
                if callable(getattr(msg, "is_system", None)) and msg.is_system():
                    return False
            except Exception:
                pass
            # skip if author is a system user
            if getattr(getattr(msg, "author", None), "system", False):
                return False
            # allow only specific message types
            if getattr(msg, "type", None) not in ALLOWED_TYPES:
                return False
            return True

        sent = 0
        last_ping = 0.0

        try:
            if getattr(self, "do_precount", False):
                total = 0
                async for m in ch.history(limit=None, oldest_first=True):
                    if not _is_normal(m):
                        continue
                    total += 1
                await self.ws.send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "count": total},
                    }
                )
            async for m in ch.history(limit=None, oldest_first=True):
                if not _is_normal(m):
                    continue

                raw = m.content or ""
                system = getattr(m, "system_content", "") or ""
                content = system if (not raw and system) else raw
                author = "System" if (not raw and system) else m.author.name

                # Embeds & mentions
                raw_embeds = [e.to_dict() for e in m.embeds]
                mention_map = await self._build_mention_map(m, raw_embeds)
                embeds = [
                    self._sanitize_embed_dict(e, m, mention_map) for e in raw_embeds
                ]
                content = self._sanitize_inline(content, m, mention_map)

                # Stickers payload
                def _enum_int(val, default=0):
                    v = getattr(val, "value", val)
                    try:
                        return int(v)
                    except Exception:
                        return default

                def _sticker_url(s):
                    u = getattr(s, "url", None)
                    if not u:
                        asset = getattr(s, "asset", None)
                        u = getattr(asset, "url", None) if asset else None
                    return str(u) if u else ""

                stickers_payload = []
                for s in getattr(m, "stickers", []) or []:
                    stickers_payload.append(
                        {
                            "id": s.id,
                            "name": s.name,
                            "format_type": _enum_int(getattr(s, "format", None), 0),
                            "url": _sticker_url(s),
                        }
                    )

                payload = {
                    "type": "message",
                    "data": {
                        "channel_id": m.channel.id,
                        "channel_name": m.channel.name,
                        "channel_type": m.channel.type.value,
                        "author": author,
                        "author_id": m.author.id,
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

                await self.ws.send(payload)

                sent += 1
                now = asyncio.get_event_loop().time()
                if sent % 50 == 0 or (now - last_ping) >= 2.0:
                    await self.ws.send(
                        {
                            "type": "backfill_progress",
                            "data": {"channel_id": original_channel_id, "count": sent},
                        }
                    )
                    last_ping = now

        except Forbidden as e:
            # Prevent the unhandled task exception and report cleanly
            logger.info(
                "[backfill] history forbidden channel=%s: %s", original_channel_id, e
            )
        except HTTPException as e:
            logger.warning(
                "[backfill] HTTP error channel=%s: %s", original_channel_id, e
            )
        finally:
            await self.ws.send(
                {"type": "backfill_done", "data": {"channel_id": original_channel_id}}
            )

    async def _shutdown(self):
        """
        Asynchronously shuts down the client.
        """
        logger.info("Shutting down client…")
        self.ws.begin_shutdown()
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
        # close the discord client last
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
        logger.info("[✨] Starting Copycord Client %s", self.config.CURRENT_VERSION)
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


if __name__ == "__main__":
    ClientListener().run()
