# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import os
import logging
from typing import Optional
from common.db import DBManager

logger = logging.getLogger(__name__)
CURRENT_VERSION = "v2.1.2"


class Config:
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
    ):
        self._release_interval = 1810

        self.DEFAULT_WEBHOOK_AVATAR_URL = "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/logo/logo.png"
        self.SERVER_TOKEN = os.getenv("SERVER_TOKEN")
        self.CLONE_GUILD_ID = os.getenv("CLONE_GUILD_ID", "0")
        self.DB_PATH = os.getenv("DB_PATH", "/data/data.db")
        self.SERVER_WS_HOST = os.getenv("SERVER_WS_HOST", "server")
        self.SERVER_WS_PORT = int(os.getenv("SERVER_WS_PORT", "8765"))
        self.SERVER_WS_URL = os.getenv(
            "WS_SERVER_URL", f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )
        self.ADMIN_WS_URL = (
            os.getenv("ADMIN_WS_URL")
            or f"ws://{os.getenv('ADMIN_HOST', 'admin')}:{os.getenv('ADMIN_PORT', '8080')}/bus"
        )

        self.ENABLE_CLONING = os.getenv("ENABLE_CLONING", "true").lower() in (
            "1",
            "true",
            "yes",
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
        self.CLONE_EMOJI = os.getenv("CLONE_EMOJI", "true").lower() in (
            "1",
            "true",
            "yes",
        )

        self.CLONE_STICKER = os.getenv("CLONE_STICKER", "true").lower() in (
            "1",
            "true",
            "yes",
        )

        self.CLONE_ROLES = os.getenv("CLONE_ROLES", "true").lower() in (
            "1",
            "true",
            "yes",
        )

        self.MIRROR_ROLE_PERMISSIONS = os.getenv(
            "MIRROR_ROLE_PERMISSIONS", "true"
        ).lower() in ("1", "true", "yes", "y", "on")

        self.DELETE_ROLES = os.getenv("DELETE_ROLES", "true").lower() in (
            "1",
            "true",
            "yes",
        )

        self.logger = (logger or logging.getLogger(__name__)).getChild(
            self.__class__.__name__
        )
        self.excluded_category_ids: set[int] = set()
        self.excluded_channel_ids: set[int] = set()

        self.db = DBManager(self.DB_PATH)
        self._load_filters_from_db()
        raw = os.getenv("COMMAND_USERS", "")
        self.COMMAND_USERS = [int(u) for u in raw.split(",") if u.strip()]

        self.CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")
        self.HOST_GUILD_ID = os.getenv("HOST_GUILD_ID", "0")
        self.CLIENT_WS_HOST = os.getenv("CLIENT_WS_HOST", "client")
        self.CLIENT_WS_PORT = int(os.getenv("CLIENT_WS_PORT", "8766"))
        self.CLIENT_WS_URL = os.getenv(
            "WS_CLIENT_URL", f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )
        self.SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "3600"))

    async def setup_release_watcher(self, receiver, should_dm: bool = True):
        await receiver.bot.wait_until_ready()
        db = receiver.db

        import re

        def _norm_version(tag: str) -> str:
            if not tag:
                return "0.0.0"
            tag = tag.strip()
            if tag.lower().startswith("v"):
                tag = tag[1:]
            tag = re.sub(r"[^0-9.]", "", tag)
            parts = [p for p in tag.split(".") if p.isdigit()]
            while len(parts) < 3:
                parts.append("0")
            return ".".join(parts[:3])

        def _ver_tuple(tag: str) -> tuple[int, int, int]:
            a, b, c = _norm_version(tag).split(".")
            return int(a), int(b), int(c)

        def _cmp_versions(a: str, b: str) -> int:
            ta, tb = _ver_tuple(a), _ver_tuple(b)
            return (ta > tb) - (ta < tb)

        async def _maybe_update_status(text: str):
            """Only server has update_status; client does not."""
            fn = getattr(receiver, "update_status", None)
            if callable(fn):
                try:
                    await fn(text)
                except Exception:
                    self.logger.debug("update_status failed", exc_info=True)
            else:
                self.logger.debug("Skipping status update (receiver has no update_status)")

        while not receiver.bot.is_closed():
            try:
                guild_id = getattr(receiver, "clone_guild_id", None) or getattr(
                    receiver, "host_guild_id", None
                )

                try:
                    if db.get_version() != CURRENT_VERSION:
                        db.set_version(CURRENT_VERSION)
                    running_ver = db.get_version()
                except AttributeError:
                    current_in_cfg = db.get_config("current_version", "")
                    if current_in_cfg != CURRENT_VERSION:
                        db.set_config("current_version", CURRENT_VERSION)
                    running_ver = CURRENT_VERSION

                latest_tag = (db.get_config("latest_tag") or "").strip()
                latest_url = db.get_config("latest_url") or ""

                if not latest_tag:
                    self.logger.debug("No latest_tag in db_config yet; skipping this cycle")
                    await _maybe_update_status(f"{running_ver}")
                    await asyncio.sleep(self._release_interval)
                    continue

                cmp_remote_local = _cmp_versions(latest_tag, running_ver)
                last_seen = db.get_notified_version() or ""

                if _norm_version(latest_tag) != _norm_version(last_seen):
                    self.logger.debug("[📢] latest_tag observed from DB: %s (%s)", latest_tag, latest_url)

                if cmp_remote_local > 0:
                    self.logger.info("[⬆️] Update available: %s %s", latest_tag, latest_url)
                    await _maybe_update_status("New update available!")

                    if should_dm and guild_id and _norm_version(latest_tag) != _norm_version(last_seen):
                        guild = receiver.bot.get_guild(guild_id)
                        if guild:
                            try:
                                owner = guild.owner or await guild.fetch_member(guild.owner_id)
                                await owner.send(
                                    f"A new Copycord release is available: **{latest_tag}**\n{latest_url}"
                                )
                                self.logger.debug("Sent release DM to guild owner %s", owner)
                                db.set_notified_version(latest_tag)
                            except Exception as e:
                                self.logger.warning("[⚠️] Failed to send new version DM: %s", e)
                else:
                    await _maybe_update_status(f"{running_ver}")

                    if cmp_remote_local == 0 and _norm_version(latest_tag) != _norm_version(last_seen):
                        db.set_notified_version(latest_tag)

                try:
                    if db.get_version() != CURRENT_VERSION:
                        db.set_version(CURRENT_VERSION)
                except AttributeError:
                    if db.get_config("current_version", "") != CURRENT_VERSION:
                        db.set_config("current_version", CURRENT_VERSION)

            except Exception:
                self.logger.exception("[⛔] Error in version release watcher loop")

            await asyncio.sleep(self._release_interval)

    def _load_filters_from_db(self):
        """
        Populate include/exclude sets from SQLite filters table.
        Whitelist is ON iff any include set is non-empty.
        """
        try:
            f = self.db.get_filters()
        except Exception:

            f = {
                "whitelist": {"category": set(), "channel": set()},
                "exclude": {"category": set(), "channel": set()},
            }

        self.include_category_ids = {int(x) for x in f["whitelist"]["category"]}
        self.include_channel_ids = {int(x) for x in f["whitelist"]["channel"]}
        self.excluded_category_ids = {int(x) for x in f["exclude"]["category"]}
        self.excluded_channel_ids = {int(x) for x in f["exclude"]["channel"]}
        self.whitelist_enabled = bool(
            self.include_category_ids or self.include_channel_ids
        )

