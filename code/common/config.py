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
import sys
import logging
from typing import Optional
import aiohttp
from common.db import DBManager

logger = logging.getLogger(__name__)
CURRENT_VERSION = "v2.0.0"


class Config:
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
    ):
        self.GITHUB_API_LATEST = (
            "https://api.github.com/repos/Copycord/Copycord/releases/latest"
        )
        self._release_interval = 3600
        # ─── Server-side settings ─────────────────────────────────
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

        self.logger = (logger or logging.getLogger(__name__)).getChild(self.__class__.__name__)
        self.excluded_category_ids: set[int] = set()
        self.excluded_channel_ids: set[int] = set()
        # DB first, YAML deprecated
        self.db = DBManager(self.DB_PATH)
        self._load_filters_from_db()
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

    async def setup_release_watcher(self, receiver, should_dm: bool = True):
        """Poll GitHub and (optionally) update presence/DM if remote > local.
        Safe to run from both server and client; client has no update_status.
        """
        await receiver.bot.wait_until_ready()
        db = receiver.db

        # Keep DB's "running version" in sync with the binary
        if db.get_version() != CURRENT_VERSION:
            db.set_version(CURRENT_VERSION)

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

                async with aiohttp.ClientSession() as session:
                    # Fetch recent releases (incl. prereleases)
                    releases = []
                    try:
                        async with session.get(
                            "https://api.github.com/repos/Copycord/Copycord/releases?per_page=20"
                        ) as resp:
                            if resp.status == 200:
                                releases = await resp.json()
                            else:
                                self.logger.debug(
                                    "Releases API %d: %s",
                                    resp.status,
                                    await resp.text(),
                                )
                    except Exception:
                        self.logger.exception("Failed to fetch releases")

                    # Fetch recent tags (extra/fallback)
                    tags = []
                    try:
                        async with session.get(
                            "https://api.github.com/repos/Copycord/Copycord/tags?per_page=20"
                        ) as resp:
                            if resp.status == 200:
                                tags = await resp.json()
                            else:
                                self.logger.debug(
                                    "Tags API %d: %s", resp.status, await resp.text()
                                )
                    except Exception:
                        self.logger.exception("Failed to fetch tags")

                # Build candidate (tag, url)
                candidates: list[tuple[str, str]] = []
                for r in releases:
                    t = (r.get("tag_name") or "").strip()
                    if t:
                        candidates.append((t, r.get("html_url") or ""))
                for t in tags:
                    name = (t.get("name") or "").strip()
                    if name:
                        candidates.append((name, t.get("zipball_url") or ""))

                if not candidates:
                    self.logger.debug("No version candidates; skipping this cycle")
                    await asyncio.sleep(self._release_interval)
                    continue

                # Highest semver from candidates
                tag, url = max(candidates, key=lambda x: _ver_tuple(x[0]))
                
                try:
                    db.set_config("latest_tag", tag or "")
                    db.set_config("latest_url", url or "")
                except Exception:
                    self.logger.debug("Failed to persist latest_tag/latest_url", exc_info=True)

                # Compare GitHub latest vs our running version
                cmp_remote_local = _cmp_versions(tag, CURRENT_VERSION)

                # Visibility log if observed tag changed (independent of notifications)
                last_seen = db.get_notified_version() or ""
                if _norm_version(tag) != _norm_version(last_seen):
                    self.logger.debug("[📢] GitHub latest observed: %s (%s)", tag, url)

                if cmp_remote_local > 0:
                    # Remote > local → update available (ALWAYS log this)
                    self.logger.info("[⬆️] Update available: %s %s", tag, url)

                    # Server-only presence
                    await _maybe_update_status("New update available!")

                    # DM only if enabled and not yet notified for this specific tag
                    if (
                        should_dm
                        and guild_id
                        and _norm_version(tag) != _norm_version(last_seen)
                    ):
                        guild = receiver.bot.get_guild(guild_id)
                        if guild:
                            try:
                                owner = guild.owner or await guild.fetch_member(
                                    guild.owner_id
                                )
                                await owner.send(
                                    f"A new Copycord release is available: **{tag}**\n{url}"
                                )
                                self.logger.debug(
                                    "Sent release DM to guild owner %s", owner
                                )
                                db.set_notified_version(tag)
                            except Exception as e:
                                self.logger.warning(
                                    "[⚠️] Failed to send new version DM: %s", e
                                )
                else:
                    # Equal or ahead → show local version (server only)
                    await _maybe_update_status(f"{CURRENT_VERSION}")

                    # If we just matched GitHub, align notified_version
                    if cmp_remote_local == 0 and _norm_version(tag) != _norm_version(
                        last_seen
                    ):
                        db.set_notified_version(tag)

                # Keep DB "running version" aligned
                if db.get_version() != CURRENT_VERSION:
                    db.set_version(CURRENT_VERSION)

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
            # Safe defaults
            f = {"whitelist": {"category": set(), "channel": set()},
                "exclude":   {"category": set(), "channel": set()}}

        self.include_category_ids   = {int(x) for x in f["whitelist"]["category"]}
        self.include_channel_ids    = {int(x) for x in f["whitelist"]["channel"]}
        self.excluded_category_ids  = {int(x) for x in f["exclude"]["category"]}
        self.excluded_channel_ids   = {int(x) for x in f["exclude"]["channel"]}
        self.whitelist_enabled      = bool(self.include_category_ids or self.include_channel_ids)
