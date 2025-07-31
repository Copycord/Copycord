import asyncio
import os
import sys
import logging
import aiohttp
from common.db import DBManager

logger = logging.getLogger("config")


class Config:
    def __init__(self):
        self.CURRENT_VERSION = "v1.3.0"
        self.GITHUB_API_LATEST = (
            "https://api.github.com/repos/Copycord/Copycord/releases/latest"
        )
        self._release_interval = 3600
        # ─── Server-side settings ─────────────────────────────────
        self.SERVER_TOKEN = os.getenv("SERVER_TOKEN")
        self.CLONE_GUILD_ID = os.getenv("CLONE_GUILD_ID", "0")
        self.DB_PATH = os.getenv("DB_PATH", "/data/data.db")
        self.db = DBManager(self.DB_PATH)
        self.SERVER_WS_HOST = os.getenv("SERVER_WS_HOST", "server")
        self.SERVER_WS_PORT = int(os.getenv("SERVER_WS_PORT", "8765"))
        self.SERVER_WS_URL = os.getenv(
            "WS_SERVER_URL", f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )
        self.DELETE_CHANNELS = os.getenv("DELETE_CHANNELS", "false").lower() in (
            "1",
            "true",
            "yes",
        )
        self.DELETE_THREADS = os.getenv("DELETE_THREADS", "false").lower() in (
            "1",
            "true",
            "yes",
        )

        raw = os.getenv("COMMAND_USERS", "")
        self.COMMAND_USERS = [int(u) for u in raw.split(",") if u.strip()]

        # ─── Client-side settings ─────────────────────────────────
        self.CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")
        self.HOST_GUILD_ID = os.getenv("HOST_GUILD_ID", "0")
        self.CLIENT_WS_HOST = os.getenv("CLIENT_WS_HOST", "client")
        self.CLIENT_WS_PORT = int(os.getenv("CLIENT_WS_PORT", "8766"))
        self.CLIENT_WS_URL = os.getenv(
            "WS_CLIENT_URL", f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )
        self.SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "3600"))

        # ─── VALIDATION ─────────────────────────────────────────────────
        for name in ("SERVER_TOKEN", "CLIENT_TOKEN"):
            if not getattr(self, name):
                logger.error(f"Missing required environment variable {name}")
                sys.exit(1)

        for name in ("HOST_GUILD_ID", "CLONE_GUILD_ID"):
            raw = getattr(self, name)
            if raw is None:
                logger.error(f"Missing required Discord guild ID env var: {name}")
                sys.exit(1)
            try:
                val = int(raw)
            except (TypeError, ValueError):
                logger.error(
                    f"Discord guild ID {name} must be an integer (got {raw!r}); aborting."
                )
                sys.exit(1)
            if val <= 0:
                logger.error(
                    f"Discord guild ID {name} must be a positive integer (got {val}); shutting down."
                )
                sys.exit(1)
            setattr(self, name, val)

    def setup_release_watcher(self, receiver):
        """Start the release watcher background task"""
        logger.info("Scheduling release watcher task")
        asyncio.get_event_loop().create_task(self._release_watcher_loop(receiver))

    async def _release_watcher_loop(self, receiver):
        """Poll GitHub every hour and log when a new tag appears."""
        await receiver.bot.wait_until_ready()
        db = receiver.db
        guild_id = receiver.clone_guild_id
        
        stored = db.get_version()
        if stored != self.CURRENT_VERSION:
            db.set_version(self.CURRENT_VERSION)

        while not receiver.bot.is_closed():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.GITHUB_API_LATEST) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.debug(
                                "GitHub API returned %d, skipping: %s",
                                resp.status,
                                text,
                            )
                            await asyncio.sleep(self._release_interval)
                            continue
                        release = await resp.json()

                tag = release.get("tag_name")
                url = release.get("html_url", "<no-url>")

                if not tag:
                    logger.debug("GitHub response missing tag_name field: %r", release)
                    await asyncio.sleep(self._release_interval)
                    continue

                last = db.get_version() # Get the current version from db
                notified = db.get_notified_version() # Check if the owner has been notified already

                if tag != last:
                    logger.info("New Copycord release detected: %s (%s)", tag, url)

                    if tag != notified:
                        guild = receiver.bot.get_guild(guild_id)
                        if guild:
                            try:
                                owner = guild.owner or await guild.fetch_member(guild.owner_id)
                                await owner.send(
                                    f"A new Copycord release is available: **{tag}**\n{url}"
                                )
                                logger.debug("Sent release DM to guild owner %s", owner)
                                db.set_notified_version(tag)
                            except Exception as e:
                                logger.warning("Failed to DM guild owner: %s", e)
                    else:
                        logger.debug("Already notified owner of %s; skipping DM", tag)

                    if tag == self.CURRENT_VERSION:
                        db.set_version(tag)

            except Exception:
                logger.exception("Error in release watcher loop")
                
            await asyncio.sleep(self._release_interval)
