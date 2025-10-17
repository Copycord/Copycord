# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
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
CURRENT_VERSION = "v2.7.0"


class Config:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.DB_PATH = os.getenv("DB_PATH", "/data/data.db")
        self.db = DBManager(self.DB_PATH)

        # --- prefer DB value if set, else environment ---
        def _get_from_db(key: str):
            try:
                return self.db.get_config(key)
            except Exception:
                return None

        def _str(key: str, env_default: Optional[str] = None) -> Optional[str]:
            v = _get_from_db(key)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                v = os.getenv(key, env_default)
            return v

        def _int(key: str, env_default: str = "0") -> int:
            raw = _str(key, env_default)
            try:
                return int(str(raw).strip())
            except Exception:
                try:
                    return int(env_default)
                except Exception:
                    return 0

        def _bool(key: str, env_default: str = "false") -> bool:
            raw = (_str(key, env_default) or "").strip().lower()
            return raw in ("1", "true", "yes", "y", "on")

        # --- Constants / misc ---
        self.RELEASE_CHECK_INTERVAL_SECONDS = 1810
        self.DEFAULT_WEBHOOK_AVATAR_URL = "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/logo/logo.png"

        # --- Tokens / IDs / URLs  ---
        self.SERVER_TOKEN = _str("SERVER_TOKEN")
        self.CLIENT_TOKEN = _str("CLIENT_TOKEN")

        self.CLONE_GUILD_ID = _str("CLONE_GUILD_ID", "0") or "0"
        self.HOST_GUILD_ID = _str("HOST_GUILD_ID", "0") or "0"

        # Websocket endpoints
        self.SERVER_WS_HOST = _str("SERVER_WS_HOST", "server") or "server"
        self.SERVER_WS_PORT = _int("SERVER_WS_PORT", "8765")
        # Back-compat: accept WS_SERVER_URL; else synthesize
        self.SERVER_WS_URL = _str(
            "WS_SERVER_URL", f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )

        self.ADMIN_WS_URL = _str(
            "ADMIN_WS_URL",
            f"ws://{os.getenv('ADMIN_HOST', 'admin')}:{os.getenv('ADMIN_PORT', '8080')}/bus",
        )

        self.CLIENT_WS_HOST = _str("CLIENT_WS_HOST", "client") or "client"
        self.CLIENT_WS_PORT = _int("CLIENT_WS_PORT", "8766")
        self.CLIENT_WS_URL = _str(
            "WS_CLIENT_URL", f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )

        self.SYNC_INTERVAL_SECONDS = _int("SYNC_INTERVAL_SECONDS", "3600")

        # --- Feature flags  ---
        self.ENABLE_CLONING = _bool("ENABLE_CLONING", "true")
        self.DELETE_CHANNELS = _bool("DELETE_CHANNELS", "false")
        self.EDIT_MESSAGES = _bool("EDIT_MESSAGES", "true")
        self.DELETE_MESSAGES = _bool("DELETE_MESSAGES", "true")
        self.DELETE_THREADS = _bool("DELETE_THREADS", "false")
        self.CLONE_EMOJI = _bool("CLONE_EMOJI", "true")
        self.CLONE_STICKER = _bool("CLONE_STICKER", "true")
        self.CLONE_ROLES = _bool("CLONE_ROLES", "true")
        self.MIRROR_ROLE_PERMISSIONS = _bool("MIRROR_ROLE_PERMISSIONS", "true")
        self.DELETE_ROLES = _bool("DELETE_ROLES", "true")
        self.MIRROR_CHANNEL_PERMISSIONS = _bool("MIRROR_CHANNEL_PERMISSIONS", "true")

        # --- Users allowed to run admin commands ---
        cmd_users_raw = _str("COMMAND_USERS", os.getenv("COMMAND_USERS", "")) or ""
        self.COMMAND_USERS = []
        for tok in str(cmd_users_raw).split(","):
            tok = tok.strip()
            if tok:
                try:
                    self.COMMAND_USERS.append(int(tok))
                except ValueError:
                    pass

        # --- Logging / misc ---
        self.logger = (logger or logging.getLogger(__name__)).getChild(
            self.__class__.__name__
        )
        self.excluded_category_ids: set[int] = set()
        self.excluded_channel_ids: set[int] = set()

        # --- Load include/exclude filters from DB ---
        self._load_filters_from_db()

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
                self.logger.debug(
                    "Skipping status update (receiver has no update_status)"
                )

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
                    self.logger.debug(
                        "No latest_tag in db_config yet; skipping this cycle"
                    )
                    await _maybe_update_status(f"{running_ver}")
                    await asyncio.sleep(self.RELEASE_CHECK_INTERVAL_SECONDS)
                    continue

                cmp_remote_local = _cmp_versions(latest_tag, running_ver)
                last_seen = db.get_notified_version() or ""

                if _norm_version(latest_tag) != _norm_version(last_seen):
                    self.logger.debug(
                        "[📢] latest_tag observed from DB: %s (%s)",
                        latest_tag,
                        latest_url,
                    )

                if cmp_remote_local > 0:
                    self.logger.info(
                        "[⬆️] Update available: %s %s", latest_tag, latest_url
                    )
                    await _maybe_update_status("New update available!")

                    if (
                        should_dm
                        and guild_id
                        and _norm_version(latest_tag) != _norm_version(last_seen)
                    ):
                        guild = receiver.bot.get_guild(guild_id)
                        if guild:
                            try:
                                owner = guild.owner or await guild.fetch_member(
                                    guild.owner_id
                                )
                                await owner.send(
                                    f"A new Copycord release is available: **{latest_tag}**\n{latest_url}"
                                )
                                self.logger.debug(
                                    "Sent release DM to guild owner %s", owner
                                )
                                db.set_notified_version(latest_tag)
                            except Exception as e:
                                self.logger.warning(
                                    "[⚠️] Failed to send new version DM: %s", e
                                )
                else:
                    await _maybe_update_status(f"{running_ver}")

                    if cmp_remote_local == 0 and _norm_version(
                        latest_tag
                    ) != _norm_version(last_seen):
                        db.set_notified_version(latest_tag)

                try:
                    if db.get_version() != CURRENT_VERSION:
                        db.set_version(CURRENT_VERSION)
                except AttributeError:
                    if db.get_config("current_version", "") != CURRENT_VERSION:
                        db.set_config("current_version", CURRENT_VERSION)

            except Exception:
                self.logger.exception("[⛔] Error in version release watcher loop")

            await asyncio.sleep(self.RELEASE_CHECK_INTERVAL_SECONDS)

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
