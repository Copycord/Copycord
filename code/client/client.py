import asyncio
import signal
from datetime import datetime, timezone
import logging
from typing import Optional
import discord
from discord import MessageType
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
    "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
root.addHandler(ch)

log_file = os.path.join(LOG_DIR, "client.log")
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
        msg is the JSON‐decoded dict sent by the server.
        """
        typ = msg.get("type")
        data = msg.get("data", {})

        if typ == "settings_update":
            self.blocked_keywords = data.get("blocked_keywords", [])
            logger.info(
                "Blocked keywords refreshed: %s",
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

    async def build_and_send_sitemap(self):
        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            logger.error("Guild %s not found", self.host_guild_id)
            return

        sitemap = {
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": [
                {
                    "id": e.id,
                    "name": e.name,
                    "url": str(e.url),
                    "animated": e.animated
                }
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

            for thread in forum.threads:
                sitemap["threads"].append(
                    {
                        "id": thread.id,
                        "forum_id": forum.id,
                        "name": thread.name,
                    }
                )

        await self.ws.send({"type": "sitemap", "data": sitemap})

        logger.info("Sitemap sent to Server.")

    async def periodic_sync_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        while True:
            try:
                await self.build_and_send_sitemap()
            except Exception:
                logger.exception("Error in periodic sync loop")
            await asyncio.sleep(self.config.SYNC_INTERVAL_SECONDS)

    async def _debounced_sitemap(self):
        try:
            await asyncio.sleep(1)
            await self.build_and_send_sitemap()
        finally:
            self.debounce_task = None

    def schedule_sync(self):
        if self.debounce_task is None:
            self.debounce_task = asyncio.create_task(self._debounced_sitemap())

    async def on_ready(self):
        # Ensure we're in the clone guild
        host_guild = self.bot.get_guild(self.host_guild_id)
        if host_guild is None:
            logger.error(
                "%s is not a member of the guild %s; shutting down.",
                self.bot.user,
                self.host_guild_id,
            )
            sys.exit(1)

        logger.info("Logged in as %s in guild %s", self.bot.user, host_guild.name)

        if self._sync_task is None:
            self._sync_task = asyncio.create_task(self.periodic_sync_loop())
        if self._ws_task is None:
            self._ws_task = asyncio.create_task(self.ws.start_server(self._on_ws))

    def extract_public_message_attrs(self, message: discord.Message) -> dict:
        """
        Return a dict of all public, non‑callable attributes on a discord.Message.
        Skips private attrs (starting with '_') and anything that raises on getattr.
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
        Skip:
        - system messages (joins/leaves/pins/etc)
        - DMs or messages not in our configured guild
        - messages containing a blocked keyword
        """
        # Ignore DMs or wrong guild
        if message.guild is None or message.guild.id != self.host_guild_id:
            return True

        # Ignore blocked keywords
        content = message.content.lower()
        for kw in self.blocked_keywords:
            if kw in content:
                logger.info(
                    "[BLOCKED] Dropping message %s: contains blocked keyword %r",
                    message.id,
                    kw,
                )
                return True

        return False

    async def on_message(self, message: discord.Message):
        if self.should_ignore(message):
            return

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

        is_thread = isinstance(message.channel, discord.Thread)
        payload = {
            "type": "thread_message" if is_thread else "message",
            "data": {
                "channel_id": message.channel.id,
                "channel_name": message.channel.name,
                "author": author,
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
                        "forum_id": message.channel.parent.id,
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
            "Forwarded msg from %s",
            message.author.name,
        )

        # Pull message attributes for debugging
        msg_attrs = self.extract_public_message_attrs(message)

        logger.debug(
            "Full Message attributes:\n%s",
            pprint.pformat(msg_attrs, indent=2, width=120),
        )

    async def on_thread_delete(self, thread: discord.Thread):
        if thread.guild.id != self.host_guild_id:
            return
        payload = {"type": "thread_delete", "data": {"thread_id": thread.id}}
        await self.ws.send(payload)
        logger.info("Notified server of deleted thread %s", thread.id)

    async def on_thread_update(self, before: discord.Thread, after: discord.Thread):
        if before.guild and before.guild.id == self.host_guild_id:
            if before.name != after.name:
                payload = {
                    "type": "thread_rename",
                    "data": {
                        "thread_id": before.id,
                        "new_name": after.name,
                    },
                }
                logger.info(
                    f"Thread rename detected: {before.id} {before.name!r} → {after.name!r}"
                )
                await self.ws.send(payload)

    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        if channel.guild.id != self.host_guild_id:
            return
        self.schedule_sync()

    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        if channel.guild.id != self.host_guild_id:
            return
        self.schedule_sync()

    async def on_guild_channel_update(self, before, after):
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
        logger.info("Starting Copycord %s", self.config.CURRENT_VERSION)
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
