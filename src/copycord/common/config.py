# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio
import os
import logging
import sqlite3
from typing import Optional

from copycord.common.db import DBManager
from copycord.common import ports
from copycord.common.paths import db_path

logger = logging.getLogger("common.config")
CURRENT_VERSION = "v4.0.0.1"


def _peer_host(_compose_name: str) -> str:
    """Default host of another role: they all share this process, so loopback."""
    return ports.LOOPBACK


class Config:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.DB_PATH = str(db_path())
        self.db = DBManager(self.DB_PATH)
        _log = logger or logging.getLogger("common.config")

        def _get_from_db(key: str):
            try:
                return self.db.get_config(key)
            except sqlite3.Error:
                _log.warning("Config %s: DB read failed, using the env value", key, exc_info=True)
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
            except ValueError:
                if raw not in (None, ""):
                    _log.warning("Config %s=%r is not a number, using %s", key, raw, env_default)
                try:
                    return int(env_default)
                except ValueError:
                    _log.debug("Config %s default %r is not a number either", key, env_default)
                    return 0

        self.RELEASE_CHECK_INTERVAL_SECONDS = 1810
        self.DEFAULT_WEBHOOK_AVATAR_URL = "https://raw.githubusercontent.com/Copycord/Copycord/refs/heads/main/assets/logo.png"

        self.SERVER_TOKEN = _str("SERVER_TOKEN")
        self.CLIENT_TOKEN = _str("CLIENT_TOKEN")

        self.SERVER_WS_HOST = _str("SERVER_WS_HOST", _peer_host("server")) or _peer_host("server")
        self.SERVER_WS_PORT = _int("SERVER_WS_PORT", str(ports.DEFAULT_SERVER_WS_PORT))

        self.SERVER_WS_URL = _str(
            "WS_SERVER_URL", f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )

        self.ADMIN_WS_URL = _str(
            "ADMIN_WS_URL",
            f"ws://{os.getenv('ADMIN_HOST', _peer_host('admin'))}:{os.getenv('ADMIN_PORT') or ports.DEFAULT_ADMIN_PORT}/bus",
        )

        self.CLIENT_WS_HOST = _str("CLIENT_WS_HOST", _peer_host("client")) or _peer_host("client")
        self.CLIENT_WS_PORT = _int("CLIENT_WS_PORT", str(ports.DEFAULT_CLIENT_WS_PORT))
        self.CLIENT_WS_URL = _str(
            "WS_CLIENT_URL", f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )


        cmd_users_raw = _str("COMMAND_USERS", os.getenv("COMMAND_USERS", "")) or ""
        self.COMMAND_USERS = []
        for tok in str(cmd_users_raw).split(","):
            tok = tok.strip()
            if tok:
                try:
                    self.COMMAND_USERS.append(int(tok))
                except ValueError:
                    pass

        self.logger = (logger or logging.getLogger("common.config")).getChild(
            self.__class__.__name__
        )
        self.excluded_category_ids: set[int] = set()
        self.excluded_channel_ids: set[int] = set()

        self._load_filters_from_db()

    def default_mapping_settings(self) -> dict:
        return {
            "ENABLE_CLONING": True,
            "CLONE_MESSAGES": True,
            "DELETE_CHANNELS": True,
            "DELETE_THREADS": True,
            "DELETE_ROLES": True,
            "UPDATE_ROLES": True,
            "DELETE_MESSAGES": True,
            "MIRROR_CHANNEL_PERMISSIONS": False,
            "CLONE_ROLES": True,
            "CLONE_EMOJI": True,
            "CLONE_STICKER": True,
            "EDIT_MESSAGES": True,
            "RESEND_EDITED_MESSAGES": True,
            "MIRROR_ROLE_PERMISSIONS": False,
            "REPOSITION_CHANNELS": True,
            "RENAME_CHANNELS": True,
            "SYNC_CHANNEL_NSFW": False,
            "SYNC_CHANNEL_TOPIC": False,
            "SYNC_CHANNEL_SLOWMODE": False,
            "REARRANGE_ROLES": False,
            "CLONE_ROLE_ICONS": False,
            "CLONE_ROLE_STYLES": False,
            "CLONE_VOICE": True,
            "CLONE_VOICE_PROPERTIES": False,
            "CLONE_STAGE": True,
            "CLONE_STAGE_PROPERTIES": False,
            "CLONE_GUILD_ICON": False,
            "CLONE_GUILD_BANNER": False,
            "CLONE_GUILD_SPLASH": False,
            "CLONE_GUILD_DISCOVERY_SPLASH": False,
            "CLONE_SERVER_TAG": False,
            "SYNC_GUILD_DESCRIPTION": False,
            "SYNC_FORUM_PROPERTIES": False,
            "ANONYMIZE_USERS": False,
            "DISABLE_EVERYONE_MENTIONS": False,
            "DISABLE_ROLE_MENTIONS": False,
            "TAG_REPLY_MSG": False,
            "APPEND_TIMESTAMP": False,
            "APPEND_AUTHOR": False,
            "ON_DEMAND_WEBHOOKS": True,
            "USE_USER_TOKENS": False,
            "USER_TOKEN_STRATEGY": "round_robin",
            "USER_TOKEN_FALLBACK_WEBHOOK": True,
            "USER_TOKEN_TYPING": False,
            "USER_TOKEN_MIN_DELAY": 0,
            "USER_TOKEN_MAX_DELAY": 0,
            "USER_TOKEN_LINKS_ONLY": False,
            "USER_TOKEN_STICKY_NICKNAME": False,
            "USER_TOKEN_STICKY_AVATAR": False,
            "USER_TOKEN_STICKY_ROLES": False,
            # off by default: proxies.txt is shared with the client, so users must opt in for token sends
            "USER_TOKEN_USE_PROXIES": False,
        }

    async def setup_release_watcher(self, receiver, should_dm: bool = True):
        await receiver.bot.wait_until_ready()
        db = receiver.db

        from copycord.update import parse_version

        def _ver_key(tag: str):
            # All four numbers count (v4.0.0.2 > v4.0.0.1); no version sorts lowest.
            return parse_version((tag or "").strip()) or ((0,),)

        def _cmp_versions(a: str, b: str) -> int:
            ta, tb = _ver_key(a), _ver_key(b)
            return (ta > tb) - (ta < tb)

        async def _maybe_update_status(text: str):
            """Only the server has update_status."""
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

        async def _notify_all_guild_owners(latest_tag: str, latest_url: str) -> bool:
            """DM each unique guild owner once. Returns True if at least one DM was sent."""
            notified_any = False
            seen_owner_ids: set[int] = set()

            for g in list(receiver.bot.guilds):

                try:
                    owner = g.owner or await g.fetch_member(g.owner_id)
                except Exception as e:
                    self.logger.warning(
                        "[⚠️] Could not resolve owner for guild %s: %s", g.id, e
                    )
                    continue

                owner_id = getattr(owner, "id", None)
                if owner_id is None:
                    self.logger.warning(
                        "[⚠️] Guild %s has no resolvable owner id, skipping DM", g.id
                    )
                    continue

                if owner_id in seen_owner_ids:
                    continue

                try:
                    await owner.send(
                        f"A new Copycord release is available: "
                        f"`{latest_tag}`\n{latest_url}"
                    )
                    self.logger.info(
                        "[⬆️] Sent release DM to guild owner %s (guild %s)",
                        owner_id,
                        g.id,
                    )
                    seen_owner_ids.add(owner_id)
                    notified_any = True

                except Exception as e:

                    self.logger.warning(
                        "[⚠️] Failed to DM owner %s in guild %s about %s: %s",
                        owner_id,
                        g.id,
                        latest_tag,
                        e,
                    )

            return notified_any

        while not receiver.bot.is_closed():
            try:

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
                last_seen = db.get_notified_version() or ""

                if not latest_tag:
                    self.logger.debug(
                        "No latest_tag in db_config yet; skipping this cycle"
                    )
                    await _maybe_update_status(f"{running_ver}")
                    await asyncio.sleep(self.RELEASE_CHECK_INTERVAL_SECONDS)
                    continue

                cmp_remote_local = _cmp_versions(latest_tag, running_ver)

                is_new_to_us = _ver_key(latest_tag) != _ver_key(last_seen)

                if cmp_remote_local > 0:
                    self.logger.info(
                        "[⬆️] Update available: %s %s", latest_tag, latest_url
                    )

                    await _maybe_update_status("New update available!")

                    if should_dm and is_new_to_us:
                        sent_any = await _notify_all_guild_owners(
                            latest_tag, latest_url
                        )
                        if sent_any:
                            db.set_notified_version(latest_tag)
                else:
                    await _maybe_update_status(f"{running_ver}")

                    if cmp_remote_local == 0 and is_new_to_us:
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
        """Populate include/exclude sets from the filters table; the whitelist is on iff any include set is non-empty."""
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
