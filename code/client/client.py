import asyncio
import re
import signal
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional
import discord
from discord import ChannelType, MessageType, Member
import os
import pprint
import sys
from discord.ext import commands
from common.config import Config
from common.db import DBManager
from common.websockets import WebsocketManager


LOG_DIR = "/data"
os.makedirs(LOG_DIR, exist_ok=True)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
root.addHandler(ch)

log_file = os.path.join(LOG_DIR, "client.log")
fh = RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,
    backupCount=1,
    encoding="utf-8",
)
fh.setLevel(logging.INFO)
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

logging.getLogger("discord.state").setLevel(logging.ERROR)

logging.getLogger("discord.client").setLevel(logging.ERROR)
logger = logging.getLogger("client")


class ClientListener:
    def __init__(self):
        self.config = Config()
        self.db = DBManager(self.config.DB_PATH)
        self.host_guild_id = int(self.config.HOST_GUILD_ID)
        self.blocked_keywords = self.db.get_blocked_keywords()
        self.start_time = datetime.now(timezone.utc)
        self.bot = commands.Bot(command_prefix="!", self_bot=True)
        self._sync_task: Optional[asyncio.Task] = None
        self.debounce_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._m_user = re.compile(r"<@!?(\d+)>")
        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)
        self.bot.event(self.on_guild_channel_create)
        self.bot.event(self.on_guild_channel_delete)
        self.bot.event(self.on_guild_channel_update)
        self.bot.event(self.on_thread_delete)
        self.bot.event(self.on_thread_update)
        self.ws = WebsocketManager(
            send_url=self.config.SERVER_WS_URL,
            listen_host=self.config.CLIENT_WS_HOST,
            listen_port=self.config.CLIENT_WS_PORT,
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
            self.blocked_keywords = data.get("blocked_keywords", [])
            logger.info(
                "[⚙️] Blocked keywords refreshed: %s",
                ", ".join(self.blocked_keywords) or "<none>",
            )
            return None

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

        return None
    
    def _humanize_user_mentions(self, content: str, message: discord.Message) -> str:
        """Replace <@1234> (and <@!1234>) with @DisplayName based on the host guild."""

        if not content:
            return content

        # Fast path: build a dict from the known mentions in the message
        id_to_name = {}
        for m in message.mentions:  # type: ignore[attr-defined]
            if isinstance(m, Member):
                id_to_name[str(m.id)] = f"@{m.display_name or m.name}"
            else:
                # Fallback for discord.User
                id_to_name[str(m.id)] = f"@{m.name}"

        def repl(match: re.Match) -> str:
            uid = match.group(1)
            name = id_to_name.get(uid)
            if name:
                return name
            # Try to resolve if it wasn't in message.mentions (rare)
            g = message.guild
            if g:
                mem = g.get_member(int(uid))
                if mem:
                    nm = f"@{mem.display_name or mem.name}"
                    id_to_name[uid] = nm
                    return nm
            # As a last resort, leave it alone
            return match.group(0)

        return self._m_user.sub(repl, content)

    async def build_and_send_sitemap(self):
        """
        Asynchronously builds and sends a sitemap of the Discord guild to the server.
        The sitemap includes information about categories, standalone text channels, forums, threads, emojis,
        and community settings of the guild. It fetches additional thread data from the database and ensures
        all relevant information is sent to the server via a WebSocket connection.
        """
        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            logger.error("[⛔] Guild %s not found", self.host_guild_id)
            return

        sitemap = {
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": [
                {"id": e.id, "name": e.name, "url": str(e.url), "animated": e.animated}
                for e in guild.emojis
            ],
            "community": {
                "enabled": guild.features and "COMMUNITY" in guild.features,
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

        for forum in guild.forums:
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
        self.config.setup_release_watcher(self, should_dm=False)
        logger.info("[🤖] Logged in as %s in guild %s", self.bot.user, host_guild.name)
        if self._sync_task is None:
            self._sync_task = asyncio.create_task(self.periodic_sync_loop())
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

    def should_ignore(self, message: discord.Message) -> bool:
        """
        Determines whether a given Discord message should be ignored based on various conditions.
        """
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
        content = (message.content or "").lower()
        for kw in self.blocked_keywords:
            if kw in content:
                logger.info(
                    "[❌] Dropping message %s: contains blocked keyword %r",
                    message.id,
                    kw,
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

    async def on_message(self, message: discord.Message):
        """
        Handles incoming Discord messages and processes them for forwarding.
        This method is triggered whenever a message is sent in a channel the bot has access to.
        """
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

        embeds = [embed.to_dict() for embed in message.embeds]

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
        content = self._humanize_user_mentions(content, message) # Replace user mentions with display names
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
        msg_attrs = self.extract_public_message_attrs(message)

        logger.debug(
            "Full Message attributes:\n%s",
            pprint.pformat(msg_attrs, indent=2, width=120),
        )

    async def on_thread_delete(self, thread: discord.Thread):
        """
        Event handler that is triggered when a thread is deleted in a Discord server.

        This method checks if the deleted thread belongs to the host guild. If it does,
        it sends a notification payload to the WebSocket server with the thread's ID.
        """
        if thread.guild.id != self.host_guild_id:
            return
        payload = {"type": "thread_delete", "data": {"thread_id": thread.id}}
        await self.ws.send(payload)
        logger.info("[📩] Notified server of deleted thread %s", thread.id)

    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        """
        Handles updates to a Discord thread, such as renaming.

        This method is triggered when a thread is updated in a guild. It checks if the
        thread belongs to the specified host guild and if the thread's name has changed.
        If a rename is detected, it constructs a payload with details about the change
        and sends it through the WebSocket connection.
        """
        if before.guild and before.guild.id == self.host_guild_id:
            if before.name != after.name:
                payload = {
                    "type": "thread_rename",
                    "data": {
                        "thread_id": before.id,
                        "new_name": after.name,
                        "old_name": before.name,
                        "parent_name": after.parent.name,
                        "parent_id": after.parent.id,
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
        if channel.guild.id != self.host_guild_id:
            return
        self.schedule_sync()

    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """
        Event handler that is triggered when a guild channel is deleted.
        """
        if channel.guild.id != self.host_guild_id:
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
        if before.guild.id != self.host_guild_id:
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

    async def _shutdown(self):
        """
        Asynchronously shuts down the client.
        """
        logger.info("Shutting down client…")
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

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
